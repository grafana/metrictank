# Consolidation

When you request a larger number of points then what is in `maxDataPoints`, the data needs to be consolidated (aggregated)
There's two pieces to this puzzle - rollups and runtime consolidation - explained below.

In metrictank, runtime consolidation works in concert with the rollup archives (in contrast to whisper and other more limited backends where you can configure only one given roll-up function for each series which [often leads to nonsense when combined with runtime consolidation](https://blog.raintank.io/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation))

By default, metrictank will consolidate (at query time) like so:

* max if mtype is `counter`.
* avg for everything else.

But you can override this
(see [HTTP api](https://github.com/raintank/metrictank/blob/master/docs/http-api.md)) to use avg, min, max, sum.
Which ever function is used, metrictank will select the appropriate rollup band, and if necessary also perform runtime consolidation to further reduce the dataset.


## Rollups
Rollups are additional archive series that are automatically created for each input series and stored in memory and in cassandra just like any other.
We don't just store the raw data, but also statistical summaries, computed over configurable timeframes, and using different functions.

* min
* max
* sum
* count

(sum and count are used to compute the average on the fly)

Configure them using the [agg-settings in the data section of the config](https://github.com/raintank/metrictank/blob/master/docs/config.md#data)


## Runtime consolidation

This further reduces data at runtime on an as-needed basis.

It supports min, max, sum, average.


## The request alignment algorithm

Metrictank uses a function called `alignRequests` which will:

* look at all the requested timeseries and the time range
* figure out the optimal input data (out of raw and rollups) and wether or not runtime consolidation is needed, and which settings to use if so
* so that we can return all series in the same, optimal interval. Optimal meaning highest resolution but still <= maxDataPoints, using least system resources.

Note:
* this function ignores TTL values.  It is assumed you've configured sensible TTL's that are long enough to cover the timeframes during which the given bands will be chosen.
* metrictank supports different raw intervals for different metrics, but aggregation (rollup) settings are currently global for all metrics.

Terminology:

* `minInterval`: the smallest raw (native) interval out of any requested series
* `pointCount`: for a given archive (raw or rollup) of a given series, describes how many points will be needed to represent the requested time range.
* `maxDataPoints`: no more than this many points should be returned per series.  Default is 800, but can be customized through the http-api.

The algorithm works like so:

* filter out non-ready aggregation bands: they are not to be read from.
* find the highest-resolution option that will yield a pointCount <= maxDataPoints.  
  If no such option exists (e.g. all series are too high resolution for the given request) we pick the lowest resolution archive and turn on runtime consolidation.
* If the selected band is not the raw data, we apply the following **heuristic**: 
  - look at the currently selected option's `belowMaxDataPointsRatio`, defined as `maxDataPoints / pointCount`.
  - look at the one-level-higher-resolution-option's `aboveMaxDataPointsRatio`, defined as `pointCount / maxDataPoints`.
  - if the latter number is lower, we move to the higher resolution option and turn on runtime consolidation.  
  the reasoning here is that the first one was much too low resolution and it makes more sense to start with higher resolution data and incur the overhead of runtime consolidation.

Let's clarify the 3rd step with an example.
Let's say we requested a time range of 1 hour, and the options are:

| i   | span  | pointCount |
| --- | ----- | ---------- |
| 0   | 10s   | 360        |
| 1   | 600s  | 6          |
| 2   | 7200s | 0          |

if maxDataPoints is 100, then selected will initially be 1, our 600s rollups.
We then calculate the ratio between maxPoints and our
selected pointCount "6" and the previous option "360".

```
belowMaxDataPointsRatio = 100/6   = 16.67
aboveMaxDataPointsRatio = 360/100 = 3.6
```

As the maxDataPoint requested is much closer to 360 then it is to 6,
we will use the 360 raw points and do runtime consolidation.

* If the metrics have different raw intervals, and we selected the raw interval, then we change the interval to the least common multiple of the different raw intervals and turn on runtime consolidation to bridge the gap.
However, if a rollup band is available with higher resolution than this outcome, then we'll use that instead, since all series can be assumed to have the same rollup configuration.  Note that this may be a sign of a suboptimal configuration: you want raw bands to be "compatible" with each other and the rollups, and rollup bands should not be higher resolution than the raw input data.

* At this point, we now know which archives to fetch for each series and which runtime consolidation to apply, to best match the given request.

## Configuration considerations


Some considerations as you configure [the data options in the config, such as ttl and agg-settings](https://github.com/raintank/metrictank/blob/master/docs/config.md#data)

* avoid doing too many bands of data and having their TTL's be too similar, because they will all contain the most recent data and overlap.  
  Aim to achieve about 10x or more reduction from one level to the next
*


INTERVAL CHOICE
reduction should ideally be at least 10x to be worthwhile
should be able to transition from one band's maxT to the next minT
must cleanly multiply between one another (why again?)
try to minimize storage overhead of each band

SPAN CHOICE
As described in the page [Memory server](https://github.com/raintank/metrictank/blob/master/docs/memory-server.md#valid-chunk-spans), only a finite set of values are valid chunk spans. This applies to rolled up chunks as well.

RETENTION:
should at the minimum be maxT otherwise what's the point
shouldn't exceed the next band's minT, because that data wouldn't be used very much

it's useful if a retention allows to compare:
* this day with same day next week, or yesterday with yesterday last week (raw!)
* this week with same week last month
* this month with same month last year


LONGER CHUNK (vs SHORTER CHUNK):
* higher compression rate
* longer promotion time, warmup time
* max chunkspan should have an explicit cap, to ease deploys and promotions
* less overhead in memory (less bookeeping variables surrounding tsz series)
* more decoding overhead
* to accomodate newest chunk, cost of keeping 1 extra old chunk in RAM increases (or hitting cassandra more)
* for aggbands, generally means storing more data in RAM that we don't really need in ram
