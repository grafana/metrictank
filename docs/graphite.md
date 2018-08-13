# Graphite

Metrictank aims to be a drop-in replacement for Graphite, but also to address a few of Graphite's shortcomings.
Here are some important functional differences to keep in mind:
(we specifically do not go into subjective things like performance or scalability here)

* currently no support for rewriting old data; for a given key and timestamp first write wins, not last. We aim to fix this.
* timeseries can change resolution (interval) over time, they will be merged seamlessly at read time.
* multiple rollup functions are supported and can be selected via consolidateBy() at query time. (except when using functions which change the nature of the data such as perSecond() etc)
* xFilesfactor is currently not supported
* will never move observations into the past (e.g. consolidation and rollups will only cause data to get an equal or higher timestamp)
* graphite timezone defaults to Chicago, we default to server time
* many functions are not implemented yet in metrictank itself, but it autodetects this and will proxy requests it cannot handle to graphite-web
  (which then uses metrictank as a simple backend).  See below for details



## Processing functions

Metrictank aims to be able to provide as much processing power as it can: we're in the process
of implementing [Graphite's extensive processing api](http://graphite.readthedocs.io/en/latest/functions.html) into metrictank itself.
We're only just getting started, which is why metrictank will automatically proxy requests to graphite if functions are requested
that it cannot provide. You can also choose to enable unstable functions via process=any
See also:
* [HTTP api docs for render endpoint](https://github.com/grafana/metrictank/blob/master/docs/http-api.md#graphite-query-api)
* [HTTP api configuration](https://github.com/grafana/metrictank/blob/master/docs/config.md#http-api).  Note the `fallback-graphite-addr` setting.

Here are the currently included functions:

| Function name and signature                                    | Alias       | Metrictank |
| -------------------------------------------------------------- | ----------- | ---------- |
| alias(seriesList, alias) seriesList                            |             | Stable     |
| aliasByNode(seriesList, nodeList) seriesList                   | aliasByTags | Stable     |
| aliasSub(seriesList, pattern, replacement) seriesList          |             | Stable     |
| asPercent(seriesList, seriesList, nodeList) seriesList |             | Stable     |
| averageSeries(seriesLists) series                              | avg         | Stable     |
| consolidateBy(seriesList, func) seriesList                     |             | Stable     |
| countSeries(seriesLists) series                                |             | Stable     |
| derivative(seriesLists) series                                 |             | Stable     |
| diffSeries(seriesLists) series                                 |             | Stable     |
| divideSeries(dividend, divisor) seriesList                     |             | Stable     |
| divideSeriesLists(dividends, divisors) seriesList              |             | Stable     |
| exclude(seriesList, pattern) seriesList                        |             | Stable     |
| filterSeries(seriesList, func, operator, threshold) seriesList |             | Stable     |
| grep(seriesList, pattern) seriesList                           |             | Stable     |
| groupByTags(seriesList, func, tagList) seriesList              |             | Stable     |
| highest(seriesList, n, func) seriesList                        |             | Stable     |
| highestAverage(seriesList, n, func) seriesList                 |             | Stable     |
| highestCurrent(seriesList, n, func) seriesList                 |             | Stable     |
| highestMax(seriesList, n, func) seriesList                     |             | Stable     |
| isNonNull(seriesList) seriesList                               |             | Stable     |
| lowest(seriesList, n, func) seriesList                         |             | Stable     |
| lowestAverage(seriesList, n, func) seriesList                  |             | Stable     |
| lowestCurrent(seriesList, n, func) seriesList                  |             | Stable     |
| maxSeries(seriesList) series                                   | max         | Stable     |
| minSeries(seriesList) series                                   | min         | Stable     |
| multiplySeries(seriesList) series                              |             | Stable     |
| movingAverage(seriesLists, windowSize) seriesList              |             | Unstable   |
| nonNegatievDerivative(seriesList, maxValue) seriesList         |             | Stable     |
| perSecond(seriesLists) seriesList                              |             | Stable     |
| rangeOfSeries(seriesList) series                               |             | Stable     |
| removeAboveValue(seriesList, n) seriesList                     |             | Stable     |
| removeBelowValue(seriesList, n) seriesList                     |             | Stable     |
| scale(seriesList, num) series                                  |             | Stable     |
| scaleToSeconds(seriesList, seconds) series                     |             | Stable     |
| stddevSeries(seriesList) series                                |             | Stable     |
| sumSeries(seriesLists) series                                  | sum         | Stable     |
| summarize(seriesList) seriesList                               |             | Stable     |
| transformNull(seriesList, default=0) seriesList                |             | Stable     |
