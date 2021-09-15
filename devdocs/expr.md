# Request deduplication throughout the render path

# Plan construction

During plan construction, identical requests can be deduplicated.
Requests are identical based on:
* metric name/patterns or SeriesByTag() call
* the context: depending on which functions have been passed through, the from/to or pngroup may have been adjusted (assuming pre-normalization is not disabled)

Examples:
* `target=(foo.*)&target=movingAverage(consolidateBy(sum(foo.*),"max"), "1min")` are different.
  We request `foo.*` twice, but with different from/to, consolidator and pngroup.
  (also MDP optimization may play a role here, but that is experimental / should always be disabled)
* `target=foo&target=sum(foo)`. PNGroup property will be 0 and >0 respectively.
* `target=sum(foo)&target=sum(foo)`. PNGroup property will different for both targets.

If they are identical, they lead to identical expr.Req objects, and will be saved in the plan only a single time.
Importantly, equivalent requests may appear (e.g. requests for the same series, once with and once without a pngroup)

# index lookups

For the purpose of index lookups we deduplicate more aggressively and only look at query/from/to.
Other properties such as pngroup, consolidator are not relevant for this purpose.
Note that a query like `target=foo.*&target=foo.bar` will return the series foo.bar twice, these are currently not deduplicated.

# ReqMap

For each combination of a plan.Req and all its resulting series, we generate a models.Req and save it into ReqMap.
Again, these requests may only differ by pngroup, consolidator or query pattern, despite covering the same data fetch

# Request planning: planRequests()

The ReqsPlan returned by planRequests() may have equivalent or identical requests.
E.g. a PNGroup may only change the effective fetch parameters if there's multiple different series, with different intervals, with the same PNGroup, because in that case, pre-normalization may kick in, and a different archive may be selected. (or the same archive/fetch but normalization at runtime)
Also, since max-points-per-req-soft gets applied group by group, and then the singles, breaching this condition may lead to different requests.

The point being, requests may be non-identical though equivalent in the ReqsPlan.

# getTargets

As described above, the list of models.Req to fetch, may contain requests that are non-identical though equivalent

* Pattern: as described in index lookups, we may fetch the exact same data twice, if it came in via different query patterns.
* PNgroup alone does not affect fetching, though honoring the PNGroup may have had an effect on e.g. the archive to fetch or AggNum
* ConsReq: requested consolidator does not matter for fetching or any processing
* Consolidator: the consolidator may have an effect on fetching (if we query a rollup) and/or on after-fetch normalization
* AggNum: affects after-fetch runtime-normalization, but not the fetching
* MaxPoints: only for mdp optimization (ignored here, experimental)

The fields that materially affect fetching are MKey, archive, from, to (and Consolidator if a rollup is fetched)

# datamap

The datamap is a list of series, grouped by expr.Req (which tracks the requested consolidator, but not the used consolidator)
There are a few cases where different expr.Req entries may have overlapping, or even identical lists of series:
* there may be two different requests with a Pattern such as `foo.*` and `foo`
* a request like `target=foo&target=sum(foo)` or `target=sum(foo)&target=sum(foo)` (and pre-normalization enabled) led to different requests to the pngroup

Note: even if the same series is present elsewhere in the datamap, each copy is a full/independent/deep copy.

# function processing

Loading data and feeding into the function processing chain happens through FuncGet, which looks up data based on the expr.Req coming from the user, which maps 1:1 to the datamap above.
In particular: it takes into account pngroups (even if they lead to equivalent fetches and thus identical series). But each copy of a series is a distinct, deep copy.
It is important to note that any series may be processed or returned more than once. E.g. a query like `target=a&target=a`. For this reason, any function or operation such as runtime consolidation, needs to make sure not to effect the contents of the datamap.
How we solve this is described in the section "Considerations around Series changes and reuse and why we chose copy-on-write" below.

# Considerations when writing a Graphite processing function in metrictank

## constructors should return a pointer

* so that properties can be set
* we get a consistent PNGroup through the pipeline (if applicable)

(the exception here is the data loading function FuncGet() which doesn't need to set any properties)

## consider whether the function is GR, IA, a transparent or opaque aggregation, or combine different series together somehow

Such functions require special options. see [the optimizations documentation](../docs/render-path.md#optimizations)

## implement our copy-on-write approach when dealing with modifying series

See section 'Considerations around Series changes and reuse and why we chose copy-on-write' below.

* must not modify the fields of any pre-existing `models.Series`. Because these are stored in a slice, other functions may refer to the same slice and modifying them in place could potentially propagate changes to other execution branches.
  (exception: FuncGet)
* should use the pool to get new slices in which to store their new/modified datapoints.
* should add said new slices into the dataMap so that when the plan has run, and the caller calls plan.Clean(), we can return its datapoints slice to the pool.
* the other purpose of the dataMap is to add processed data to the set of available data such that other functions could reuse it, but this mechanism is not implemented yet.
  That's why we always add to the dataMap without bothering to set the right request key (`dataMap[Req{}]`).

example: an averageSeries() of 3 series:
* will create an output series value.
* it will use a new datapoints slice, retrieved from pool, because the points will be different. Also, it will allocate a new meta section and tags map because they are also different from the input series.
* won't put the 3 inputs back in the pool or dataMap, because whoever allocated the input series was responsible for doing that. We must not add the same arrays to the pool multiple times.
* It will however store the newly created series into the dataMap such that during plan cleanup time, the series' datapoints slice will be moved back to the pool.

# Considerations around Series changes and reuse and why we chose copy-on-write.

## Introduction

`plan.Run()` runs all function processing as well as any needed follup, such as runtime consolidation.
To do this, it has a dataMap as its source of data. For more information on how this structure is generated,
see above. But the TLDR is that it is the source of truth of series data, keyed by the expr.Req,
and that each entry in the datamap may be read more than once (if the same expression is used multiple times in a query)

Many processing functions (or runtime consolidation) will want to return an output series that differs from the input,
e.g. a different target or interval, tags, datapoints, etc.

This requires 2 special considerations:

1. Since in most of the processing chain, series are passed as series lists (`in []models.Series`),
any change such as `in[0].Interval = <newvalue>` will impact the entry in the datamap.
2. The `models.Series` type, even when copied or passed by value, still has a few fields that need special attention, because
  the underlying datastructures are shared. These are:
  - `Datapoints []schema.Point`
  - `Tags       map[string]string`
  - `Meta       SeriesMeta`

Thus, when making changes, we need a place to store the new output. For 1. it suffices to take a copy of the series struct itself,
for 2, we need to copy the underlying datastructures.

## Goals

* processing functions should not modify data if that data needs to remain original (e.g. because of re-use of the same input data elsewhere)
* minimize allocations of new structures foremost
* minimize data copying as a smaller concern
* simple code

# Implementation

there's 2 main choices:

1) copy-on-write:
- each function does not modify data in their inputs, they allocate new structures (or possibly get from pool) if there's differences with input
- storing output data into new slice can typically be done in same pass as processing the input data
- if you have lots of processing steps (graphite function calls) in a row, we will be creating more slices and copy more data than strictly necessary (intermediate copies. also some points may not change).
- getting a slice from the pool may cause a stall if it's not large enough and runtime needs to re-allocate and copy
- allows us to pass the same data into multiple processing steps (reuse input data)

2) copy in advance:
- provide each processing function with input series in which they can do whatever they want (e.g. modify in place)
- practically, we would create a deep copy between fetching the input series and handing it off to the first processing function.
- works well if you have many processing steps in a row that can just modify in place
- copying up front, in a separate pass. also causes a stall
- often copying may be unnessary, but we can't know that in advance (unless we expand the expr tree to mark whether it'll do a write)
- means we cannot cache intermediate results, unless we also make deep copies any time we want to cache and hand off for further processing.


For now we assume that multi-steps in a row is not that common, and COW seems more commonly the best approach, so we chose **the copy on write approach**


This leaves the problem of effectively managing allocations and using a sync.Pool.
Note that the expr library can be called by different clients. At this point only Metrictank uses it, but we intend this library to be easily embeddable in other programs. 
In order to avoid unexpected panics, a default package-global pool is initialized when package is imported, however in order to maximize efficiency, clients SHOULD instantiate and set a pool calling the `expr.Pool(...)` function.
It's up to the client to instantiate the pool, and set up the default allocation to return point slices of desired point capacity.
The client can then of course use this pool to store series, which it then feeds to expr.
expr library does the rest.  It manages the series/pointslices and gets new ones as a basis for the COW.
Once the data is returned to the client, and the client is done using the returned data, it should call plan.Clean(),
which returns all data back to the pool  (both input data or newly generated series, whether they made it into the final output or not).


# consolidateBy

consolidateBy(<foo>, "fun") defines a consolidation function to be applied to "<foo>". there are some subtle behaviors we have to cater to, and we do it like so:

consider `target=consolidateBy(divideSeries(consolidateBy(summarize(consolidateBy(foo, "min"), "5min", "min"), "avg"), bar.*), "max")`

we can visualize this as a tree:

```
------------------------------------ root of tree
|
\/
consolidateBy(
    divideSeries(
        consolidateBy(
            summarize(
                consolidateBy(
                    foo, <---------- leaf
	            "min"
                ),
		"5min",
		"min"
            ),
            "avg"
	),
        bar.* <--------------------- leaf
    ),
    "max"
)
```
There are 2 important information flows to be aware of: parsing and after it, execution.
* Parsing starts at the root and continues until leaves are resolved, and we know which series need to be fetched.
* Execution happens the other way around: first the data at the leaves is fetched, then functions are applied until we hit the root.
  At that point function processing is complete; we can do some final work (like merging of series that are the same metric but a different raw interval, and maxDatapoints consolidation) and return the data back to the user.

So:
1) at parse time, consolidation settings encountered in consolidateBy calls are passed down the tree (e.g. via the context, typically until it hits the data request.
   in some use cases, this would lead to confusing/incorrect results, so when we parse down and encounter special* functions the consolidator is reset to the default for the series
2) when reading data, we always select as high resolution data as possible.
   (this is good for functions like summarize etc but overkill in many cases where we only do basic/no processing. in such cases, if we then have to do runtime consolidation, there is room to optimize this by fetching lower resolution data in the beginning. TODO)
   and if we select a rollup for reads then we select the right archive in this order of precedence:
   - consolidateBy setting defined closest to the leaf without a special* function in between the setting and the leaf, if available
   - determined via storage-aggregation.conf (defaults to average)
3) at execution time, the consolidation settings encountered in consolidateBy calls travel up to the root because it is configured on the series, which is passed through the various layers of processing until it hits the root and the output step.  This becomes useful in two cases:
   - when series need to be normalized at runtime, e.g. for sumSeries or divideSeries with series that have different steps; they need to be normalized (consolidated) so that the series get a compatible step, and the default of "avg" may not suffice.  (note that right now we have alignRequests which normalizes all series at fetch time, which can actually be a bit too eager, because some requests can use multiple targets with different processing - e.g. feed two different series into summarize(), so we actually don't need to normalize at runtime, but in the future we should make this better - TODO THIS IS OUT OF DATE)
   - when returning data back to the user via a json response and whatnot, we can consolidate down using the method requested by the user (or average, if not specified). Likewise here, when the setting encounters a special* function while traveling up to the root, the consolidation value is reset to the default (average)
   Note: some functions combine multiple series into a new one (e.g. sumSeries, avgSeries, ...). Your input series may use different consolidateBy settings, some may be explicitly specified while others are not.  In this scenario, the output series will be given the first explicitly defined consolidateBy found by iterating the inputs, or the first default otherwise.

By combining the pass-down and pass-up we can give the user max power and correctness. In particular it also solves the problem with Graphite where data can be read from a different consolidation archive than what it used for runtime consolidation. While this is sometimes desirable (e.g. using special* functions), often - for most/simple requests - it is not. See
(see https://grafana.com/blog/2016/03/03/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)

[*] Special functions: e.g. summarize, perSecond, derivative, integral. passing consolidate settings across these function calls would lead to bad results.

see also https://github.com/grafana/metrictank/issues/463#issuecomment-275199880



# Naming

When requesting series directly, we want to show the target (to uniquely identify each series), not the queryPattern
Thus, the output returned by plan.Run must always use Target attribute to show the proper name
It follows that any processing functions must set that attribute properly.
However, some functions (e.g. sumSeries) take queryPattern of their inputs, to show summary query pattern instead of each individual value.
Thus, to accommodate wrapping functions (which may use QueryPattern) as well as lack of wrapping functions (e.g. generating json output which will look at Target), processing functions must set both Target and QueryPatt, and they should set it to the same value.

In a future version we can probably refactor things to make this simpler: fetched data could result in a series with key attribute the graphite key (currently stored in target), and store the query pattern used in the target attribute.  functions only set target and only look at target, and at output phase, we just use target. there's only 1 special case of metrics returned without function processing. we could detect this case, or if we have multiple things with same target, print the metric name instead, or something

examples: (series are in `series{target,querypatt}` form)

input              => results in fetched series                                   output targets
==================================================================================================================
target=a&target=ab => series{a, a }, series{ab,ab}                                a and ab
target=a*          => series{a, a*}, series{ab,a*}                                a and ab
target=sum(a,ab)   => series{a, a }, series{ab,ab}                                sum(a,ab)
target=sum(a*)     => series{a, a*}, series{ab,a*}                                sum(a*)
target=sum(a,a,ab) => series{a, a }, series{a,a  }, series{ab, ab}                sum(a,a,ab) (bug in graphite! it has sum(a,ab) )
target=sum(a,a*)   => series{a, a }, series{a,a* }, series{ab, a*}                sum(a,a*)
target=sum(a,a,a*) => series{a, a }, series{a,a  }, series{a, a*}, series{ab, a*} sum(a,a,a*) (bug in graphite! it has sum(a,a*), doesn't even add the extra a)
