rollups, aka storage aggregation or archival consolidation.

Metrictank can save various bands of aggregated data, using multiple consolidation functions per series. this works seamlessly with consolidateBy, unlike whisper and other more limited backends where you can configure only one given roll-up function for each series, so that if you select a different function with consolidateBy you [can get nonsense back](https://blog.raintank.io/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)

