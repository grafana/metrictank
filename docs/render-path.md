# definitions

## quantized form

a raw series is quantized when the timestamps are adjusted to be regular (at fixed intervals).
Note that some points may be null (have no data), but they should be included.
E.g. an input stream that has:
* an interval of 10
* points with timestamps 58, 67, 75, 95 

is quantized to a series with points with timestamps 60, 70, 80, 90 (null), 100.

## fixed form

a series is fixed, with respect to a query with from/to time, when
* it is quantized.
* contains all the timestamps such that `from <= timestamps < to`, but no more.

## canonical form

canonical form comes into play when we need to normalize (through consolidation of points) a series, to be at a higher interval.
It essentially means a series looks like a "native" fixed series of that higher interval,
with respect to how many points it contains and which timestamps they have.

It is important here to keep in mind that consolidated points get the timestamp of the last of its input points.

Continuing the above example, if we need to normalize the above series with aggNum 3 (OutInterval is 30s)
we would normally get a series of (60,70,80), (90 -null -, 100, 110 - null), so the 30-second timestamps become 80 and 110.
But this is not the quantized form of a series with an interval of 30.

So, what typically happens to make a series canonical, is at fetch time, also fetch some extra earlier data.
such that after aggregation, the series looks like (40, 50, 60) (70, 80, 90) (100, 110 -null -, 120 - null -) which results in timestamps 60, 90, 120.
Technically speaking we don't have to fetch the earlier points, we could leave them null, but then the data would be misleading.
It's better for the first aggregate point to have received its full input set of raw data points.

The other thing is, if the query provided a `to` of 140, a 30s series would have the point 120 as the last one as 150 is out of bounds.
But if we simply fetched our 10s series with the same `to` it would include point 130, which when consolidated results in a group of
(130, 140, 150), which would get output timestamp of 150, which should not have been included.

Thus, at fetch time we also adjust the `to` value such that no values are included that would produce an out-of-bounds timestamp after
consolidation.


Why is this important? Well I'm glad you asked!
After a serie is fetched and normalized, it is often combined with other series:

1) used in cross-serie aggregates (e.g. sumSeries).
2) merged with other series. (e.g. user changed interval of their metric and we stitch them together)

For these aggregations and merging to work well, the series need to have the same length and the same timestamps.
Note that the other series don't necessarily have to be series that are "native" series of that interval.
Continuing the example again, it could be another series that had a raw interval of 15s and is normalized with AggNum=2.

## pre-canonical

a pre-canonical series is simply a series that after normalizing, will be canonical.
I.O.W. is a series that is fetched in such a way that when it is fed to Consolidate(), will produce a canonical series.
See above for more details.
Note: this can only be done to the extent we know what the normalization looks like.
(by setting up req.AggNum and req.OutInterval for normalization). For series that get (further) normalized at runtime,
we can't predict this at fetch time and have to remove points to make the output canonical, or do what Graphite also does,
which is to add null points at the beginning or end as needed, which may lead to inaccurate leading or trailing points that
go potentially out of the bounds of the query.


## nudging

in graphite, nudging happens when doing MDP-based consolidation:
after determining the post-consolidation interval (here referred to as postInterval)
it removes a few points from the beginning of the series (if needed),
such that:
* each aggregation bucket has a full set of input points (except possibly the last one)
  (i.o.w. the first point in the series is the first point for an aggregation bucket)
* across different requests, where points arrive on the right and leave the window on the left,
  the same timestamps are always aggregated together, and the timestamp is always consistent
  and divisible by the postInterval.



In metrictank we do the same, via nudge(), invoked when doing MDP-based consolidation.
Except, when we have only few points, strict application of nudging may result in confusing,
strongly altered results. We only nudge when we have points > 2 * postAggInterval's worth.
This means that in cases of few points and a low MDP value, where we don't nudge,
we do not provide the above 2 guarantees, but a more useful result.


## normalization

given multiple series being fetched of different resolution, normalizing is runtime consolidation
but only for the purpose of bringing series of different resolutions to a common, lower resolution
such that they can be used together (for aggregating, merging, etc)

# Optimizations

Metrictank has two specific optimizations that can be enabled with the config settings:

```
[http]
# enable pre-normalization optimization
pre-normalization = true
# enable MaxDataPoints optimization (experimental)
mdp-optimization = false
```

We explain them in detail below.

## Pre-normalization

First, let's look at some definitions.

### Interval-altering function

Certain functions will return output series in an interval different from the input interval.
For example summarize() and smartSummarize(). We refer to these as IA-functions below.
In principle we can predict what the output interval will be during the plan phase, because we can parse the function arguments.
However, for simplicty, we don't implement this and treat all IA functions as functions that may change the interval of series in unpredicatable ways.

### Transparent aggregation

A transparent aggregation is a processing function that aggregates multiple series together in a predictable way (meaning: known at planning time, before fetching the data).
E.g. sumSeries, averageSeries are known to always aggregate all their inputs together.

### Opaque aggregation

An opaque aggregation is a processing function where we cannot accurately predict which series will be aggregated together
because it depends on information (e.g. names, tags) that will only be known at runtime. (e.g. groupByTags, groupByNode(s))

### Pre-normalizable

In the past, metrictank used to always align all series to the same resolution. But that was a limitation and we don't do this anymore (#926).
Generally, if series have different intervals, they can keep those and we return them in whichever interval works best for them.

However, when data will be used together (e.g. aggregating multiple series together, or certain functions like divideSeries, asPercent, etc) they will need to have the same interval.
An aggregation can be opaque or transparent as defined above.

Pre-normalizing is when we can safely - during planning - set up normalization to happen right after fetching (or better: set up the fetch parameters such that normalizing is not needed) and wen we know the normalization won't affect anything else.
This is the case when series go from fetching to transparent aggregation, possibly with some processing functions - except opaque aggregation(s) or IA-function(s) - in between, and
with asPercent in a certain mode (where it has to normalize all inputs), but not with divideSeries where it applies the same divisor to multiple dividend inputs, for example.

For example if we have these schemas:
```
series A: 1s:1d,10s:1y
series B: 10s:1d
```

Let's say the initial fetch parameters are to get the raw data for both A and B.
If we know that these series will be aggregated together, they will need to be normalized, meaning A will need to be at 10s resolution.
If the query is `sum(A,B)` or `sum(perSecond(A),B)` we can safely pre-normalize, specifically, we can fetch the first rollup of series A, rather than fetching the raw data
and then normalizing (consolidating) at runtime - and thus spend less resources - because we know for sure that having the coarser data for A will not cause trouble in this pipeline.
However, if the query is `sum(A, summarize(B,...))` we cannot safely do this as we don't have a prediction of what the output interval of `summarize(B,...)` will be.
Likewise, if the query is `groupByNode(group(A,B), 2, callback='sum')` we cannot predict whether A and B will end up in the same group, and thus should be normalized.

Benefits of this optimization:
1) less work spent consolidating at runtime, less data to fetch
2) it assures data will be fetched in a pre-canonical way. If we don't set up normalization for fetching, data may not be pre-canonical, such that
3) pre-normalized data reduces a request's chance of breaching max-points-per-req-soft and thus makes it less likely that other data that should be high-resolution gets fetched in a coarser way.
when it eventually needs to be normalized at runtime, points at the beginning or end of the series may be less accurate.

Downsides of this optimization:
1) if you already have the raw data cached, and the rollup data is not cached yet, it may result in a slower query.  But this is an edge case
2) uses slightly more of the chunk cache.

## MDP-optimizable

### Greedy-resolution functions

A Greedy-resolution function (GR-function) is a certain processing function that requires, or may require, high resolution data input to do their computations, even if their output will be consolidated down (due to maxDatapoints setting)
For example summarize().
For these, we should return as high-resolution data as we can.

### MDP-optimizable

MDP-optimizable aka maxDataPoints-optimizable is a data request where we can safely fetch lower precision data by taking into account MaxDataPoints-based consolidation that will take place after function processing.
A data request is currently considered MDP-optimizable if we know for sure that it won't be subjected to GR-functions.
I.O.W. when both of these conditions are true:
* the client was an end-user, not Graphite (Graphite may run any processing, such as GR-functions, without telling us)
* we (metrictank) will not run GR-functions on this data

What kind of optimizations can we do? Consider this retention rule:

`1s:1d,10s:1y`
request from=now-2hours to=now, MDP=800
Our options are:
* 7200 raw (archive 0) datapoints, consolidate aggNum 9, down to 800 (by the way, current code does generate "odd" intervals like 9s in this case)
* 720 datapoints of archive 1.

While archive 1 is a bit less accurate, it is less data to load, decode, and requires no consolidation. We have a strong suspicion that it is less costly to use this data to satisfy the request.

(a bit of history here: in the early days, we used to always apply basically this optimization to *all* requests. This turned out to be a bad idea when we realized what happened with GR-functions.  In #463 we decided to simply disable all optimizations and always fetch raw data for everything. This assured correctness, but also was needlessly aggressive for certain types of requests discussed here.)

However, there are a few concerns not fully fleshed out.
* Targeting a number of points of MDP/2 seems fine for typical charts with an MDP of hundreds or thousands of points. Once people request values like MDP 1, 2 or 3 it becomes icky.
* For certain queries like `avg(consolidateBy(seriesByTags(...), 'max'))` or `seriesByTag('name=requests.count') | consolidateBy('sum') | scaleToSeconds(1) | consolidateBy('max')`, that have different consolidators for normalization and runtime consolidation, would results in different responses.  This needs more fleshing out, and also reasoning through how processing functions like perSecond(), scaleToSeconds(), etc may affect the decision.

For this reason, this optimization is **experimental** and disabled by default.
