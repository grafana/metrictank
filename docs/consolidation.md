When you request a larger amount of points then what is in `maxDataPoints`, the data needs to be consolidated (aggregated)
There's two pieces to this puzzle - rollups and runtime consolidation - explained below.

In metrictank, runtime consolidation works in concert with the rollup archives, in contrast to whisper and other more limited backends where you can configure only one given roll-up function for each series, so that if you select a different function with consolidateBy for runtime consolidation, you [can get nonsense back](https://blog.raintank.io/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)

By default, metrictank will consolidate (at query time) like so:

* max if targetType is `counter`.
* avg for everything else.

But you can override this (see http-api.md) to use avg, min, max, sum.
Which ever function is used, metrictank will select the appropriate rollup band, and if necessary also perform runtime consolidation to further reduce the dataset.


# rollups
Rollups are additional archive series that are automatically created for each input series and stored in memory and in cassandra just like any other.
We don't just store the raw data, but also statistical summaries, computed over configurable timeframes, and using different functions.

* min
* max
* sum
* count

(sum and count are used to compute the average on the fly)


# runtime consolidation
This further reduces data at runtime on an as-needed basis

# considerations
TODO: parameters, heuristic.
