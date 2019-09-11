# definitions

## quantized form

a raw is quantized when the timestamps are adjusted to be regular.
E.g. an input stream that has:
* an interval of 10
* points with timestamps 58, 67, 75, 95 

is quantized to a series with points with timestamps 60, 70, 80, 90 (this one has no data), 100.

## fixed form

a series is fixed, with respect to a query with from/to time, when
* it is quantized.
* contains a point for each interval (possibly null).
* contains only timestamps such that `from <= timestams < to`.

## canonical form

canonical form comes into play when we need to normalize (through consolidation of points) a series, to be at a higher interval.
It essentially means a series looks like a "native" fixed series of that higher interval,
with respect to how many points it contains and which timestamps they have.

It is important here to keep in mind that consolidated points get the timestamp of the last of its input points.

Continuing the above example, if we need to normalize the above series with aggNum 3 (OutInterval is 30s)
We would normally get a series of (60,70,80), (90, 100, 110 - this one has no data), so 80, 110.
But this is not the quantized form of a series with an interval of 30.

So, what typically happens to make a series canonical, is at fetch time, also fetch some extra earlier data.
such that after aggregation, the series looks like (40, 50, 60) (70, 80, 90) (100, 110 -no data, 120 - no data) or 60, 90, 120.
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

## nudging
in graphite, nudging happens when doing MDP-based consolidation:
after determining the post-consolidation interval (here referred to as postInterval)
it removes a few points from the beginning of the series (if needed),
such that:
* each aggregation bucket has a full set of input points (except possibly the last one)
  (i.o.w. the first point in the series is the first point for an aggregation bucket)
* across different requests, where points arrive on the right and leave the window on the left,
  the same timestamps are always aggregated together, and the timestamp is always consistent
  and diviseble by the postInterval.



In metrictank we do the same, via nudge(), invoked when doing MDP-based consolidation.
Except, when we have only few points, strict applicating of nudging may result in confusing,
strongly altered results. We only nudge when we have points > 2 * postAggInterval's worth.
This means that in cases of few points and a low MDP value, where we don't nudge,
we do not provide the above 2 guarantees, but a more useful result.


## normalizing

given multiple series being fetched of different resolution, normalizing is runtime consolidation
but only for the purpose of bringing series of different resolutions to a common, lower resolution
such that they can be used together (for aggregating, merging, etc)


## request flow


TODO talk about
alignRequests -> getTargets -> mergeSeries -> sort Series -> plan.Run (executes functions and does MDP consolidation with nudging)

talk more about what happens at each step, how data is manipulated etc
