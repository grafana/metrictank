
# http interface

* to query for series data
* `http://localhost:6063/get?render=<id>` (render variable can be given multiple times for several series)
* optionally, specify `from` and `to` unix timestamps.
  * from is inclusive, to is exclusive.
  * so from=x, to=y returns data that can include x and y-1 but not y.
  * from defaults to now-24h, to to now+1.

note:
* it just serves up the data that it has, in timestamp ascending order. it does no effort to try to fill in gaps.
* no support for wildcards, patterns, "magic" time specs like "-10min" etc.

# aggregations

the aggregated metrics are not accessible through http ui, and that's probably fine, cause we'll only query for old aggregated data anyway, in cassandra
making it accessible would be extra work and resources for no good reason
TODO: you can currently write fake metrics with same key as aggregated metrics, which would conflict, we should probably blacklist such patterns
