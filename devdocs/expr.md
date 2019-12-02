Considerations when writing function:
make sure to return pointer, so that properties can be set, and we get a consistent PNGroup through the pipeline (if applicable)
(the exception here is the data loading function FuncGet() which doesn't need to set any properties)
consider whether the function is GR, IA, a transparant or opaque aggregation. because those require special options. see https://github.com/grafana/metrictank/issues/926#issuecomment-559596384
make sure to do the right things wrt getting slicepools, adding them to the cache for cleanup, not modifying tags, etc. (see below re memory management)


## MDP-optimization

MDP at the leaf of the expr tree (fetch request) 0 means don't optimize, set it to >0 means, can be optimized.
When the data may be subjected to a GR-function, we set it to 0.
How do we achieve this?
* MDP at the root is set 0 if request came from graphite or to MaxDataPoints otherwise.
* as the context flows from root through the processing functions to the data requests, if we hit a GR function, we set to MDP to 0 on the context (and thus also on any subsequent requests)

## Pre-normalization

Any data requested (checked at the leaf node of the expr tree) should have its own independent interval.
However, multiple series getting fetched that then get aggregated together, may be pre-normalized if they are part of the same pre-normalization-group. ( have a common PNGroup that is > 0 )
(for more details see devdocs/alignrequests-too-course-grained.txt)
The mechanics here are:
* we set PNGroup to 0 by default on the context, which gets inherited down the tree
* as we traverse down tree: transparant aggregations set PNGroups to the pointer value of that function, to uniquely identify any further data requests that will be fed into the same transparant aggregation.
* as we traverse down, any opaque aggregation functions and IA-functions reset PNGroup back to 0.

## Management of point slices

The `models.Series` type, even when passed by value, has a few fields that need special attention:
* `Datapoints []schema.Point`
* `Tags       map[string]string`
* `Meta       SeriesMeta`

Many processing functions will want to return an output series that differs from the input, in terms of (some of the) datapoints may have changed value, tags or metadata.
They need a place to store their output but we cannot simply operate on the input series, or even a copy of it, as the underlying datastructures are shared.

Goals:
* processing functions should not modify data if that data needs to remain original (e.g. because of re-use of the same input data elsewhere)
* minimize allocations of new structures foremost
* minimize data copying as a smaller concern
* simple code

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
- means we cannot cache intermediate results, unless we also make deep copies anny time we want to cache and hand off for further processing.


for now we assume that multi-steps in a row is not that common, and COW seems more commonly the best approach, so we chose COW.


This leaves the problem of effectively managing allocations and using a sync.Pool.
Note that the expr library can be called by different clients (MT, grafana, graphite-ng, ...)
It's up to the client to instantiate the pool, and set up the default allocation to return point slices of desired point capacity.
The client can then of course use this pool to store series, which it then feeds to expr.
expr library does the rest.  It manages the series/pointslices and gets new ones as a basis for the COW.
Once the data is returned to the client, and the client is done using the returned data, it should call plan.Clean(),
which returns all data back to the pool  (both input data or newly generated series, whether they made it into the final output or not).


function implementations:

* must not modify existing slices or maps or other composite datastructures (at the time of writing, it's only slices/maps), with the exception of FuncGet.
* should use the pool to get new slices in which to store their new/modified datapoints.
* should add said new slices into the cache so it can later be cleaned

example: an averageSeries() of 3 series:
* will create an output series value.
* it will use a new datapoints slice, retrieved from pool, because the points will be different. also it will allocate a new meta section and tags map because they are different from the input series also.
* won't put the 3 inputs back in the pool, because whoever allocated the input series was responsible for doing that. we should not add the same arrays to the pool multiple times.
* It will however store the newly created series into the pool such that it can later be reclaimed.

## consolidateBy

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
There's 2 important information flows to be aware of: parsing and after it, execution.
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
   - when series need to be normalized at runtime, e.g. for sumSeries or divideSeries with series that have different steps; they need to be normalized (consolidated) so that the series get a compatible step, and the default of "avg" may not suffice.  (note that right now we have alignRequests which normalizes all series at fetch time, which can actually be a bit too eager, because some requests can use multiple targets with different processing - e.g. feed two different series into summarize(), so we actually don't need to normalize at runtime, but in the future we should make this better - TODO)
   - when returning data back to the user via a json response and whatnot, we can consolidate down using the method requested by the user (or average, if not specified). Likewise here, when the setting encounters a special* function while traveling up to the root, the consolidation value is reset to the default (average)
   Note: some functions combine multiple series into a new one (e.g. sumSeries, avgSeries, ...). Your input series may use different consolidateBy settings, some may be explicitly specified while others are not.  In this scenario, the output series will be given the first explicitly defined consolidateBy found by iterating the inputs, or the first default otherwise.

By combining the pass-down and pass-up we can give the user max power and correctness. In particular it also solves the problem with Graphite where data can be read from a different consolidation archive than what it used for runtime consolidation. While this is sometimes desirable (e.g. using special* functions), often - for most/simple requests - it is not. See
(see https://grafana.com/blog/2016/03/03/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)

[*] Special functions: e.g. summarize, perSecond, derivative, integral. passing consolidate settings across these function calls would lead to bad results.

see also https://github.com/grafana/metrictank/issues/463#issuecomment-275199880



## naming

when requesting series directly, we want to show the target (to uniquely identify each series), not the queryPattern
thus the output returned by plan.Run must always use Target attribute to show the proper name
it follows that any processing functions must set that attribute properly.
but some functions (e.g. sumSeries) take queryPattern of their inputs, to show summary query pattern instead of each individual value.
thus, to accommodate wrapping functions (which may use QueryPattern) as well as lack of wrapping functions (e.g. generating json output which will look at Target), processing functions must set both Target and QueryPatt, and they should set it to the same value.

in a future version we can probably refactor things to make this simpler: fetched data could result in a series with key attribute the graphite key (currently stored in target), and store the query pattern used in the target attribute.  functions only set target and only look at target, and at output phase, we just use target. there's only 1 special case of metrics returned without function processing. we could detect this case, or if we have multiple things with same target, print the metric name instead, or something

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
