
# http interface

## data querying
* `http://localhost:6063/get` either POST or GET, with the following parameters:
  * `target` mandatory. can be specified multiple times to request several series. Supported formats:
    * simply the raw id of a metric. like `1.2345foobar`
    * `consolidateBy(<id>,'<function>')`. single quotes only. accepted functions are avg, average, last, min, max, sum.
       example: `consolidateBy(1.2345foobar,'average')`.
  * `maxDataPoints`: max points to be returned. runs runtime consolidation when needed. optional
  * `from` and `to` unix timestamps. optional
    * from is inclusive, to is exclusive.
    * so from=x, to=y returns data that can include x and y-1 but not y.
    * from defaults to now-24h, to to now+1.

* the response will id the series by the target used to request them

note:
* it just serves up the data that it has, in timestamp ascending order. it does no effort to try to fill in gaps.
* no support for wildcards, patterns, "magic" time specs like "-10min" etc.

## other useful endpoints exemplified through curl commands:

* `curl http://localhost:6063/` app status (OK if either primary or secondary that has been warmed up). good for loadbalancers.
* `curl http://localhost:6063/cluster` cluster status
* `curl -X POST -d primary=false http://localhost:6063/cluster` set primary true/false


# aggregations

MT can save various bands of aggregated data, using multiple consolidation functions per series. this works seamlessly with consolidateBy, unlike graphite.

TODO: you can currently write fake metrics with same key as aggregated metrics, which would conflict, we should probably blacklist such patterns

`
