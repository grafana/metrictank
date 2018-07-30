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

Below is an overview of all current Graphite functions (as of Aug 15, 2018) and their support in metrictank.
There are 3 levels of support:

* No : not implemented yet in metrictank or not applicable (e.g. graphite functions that affect graphical style but not json data)
* Stable : 100% compatible with graphite and vetted
* Unstable: not fully compatible yet or not vetted enough

When you request functions that metrictank cannot provide, it will automatically proxy requests to graphite for a seamless failover.
You can also choose to enable unstable functions via process=any
See also:
* [HTTP api docs for render endpoint](https://github.com/grafana/metrictank/blob/master/docs/http-api.md#graphite-query-api)
* [HTTP api configuration](https://github.com/grafana/metrictank/blob/master/docs/config.md#http-api).  Note the `fallback-graphite-addr` setting.


| Function name and signature                                    | Alias        | Metrictank |
| -------------------------------------------------------------- | ------------ | ---------- |
| absolute                                                       |              | No         |
| aggregate                                                      |              | No         |
| aggregateLine                                                  |              | No         |
| aggregateWithWildcards                                         |              | No         |
| alias(seriesList, alias) seriesList                            |              | Stable     |
| aliasByMetric                                                  |              | No         |
| aliasByNode(seriesList, nodeList) seriesList                   | aliasByTags  | Stable     |
| aliasByTags                                                    |              | No         |
| aliasQuery                                                     |              | No         |
| aliasSub(seriesList, pattern, replacement) seriesList          |              | Stable     |
| alpha                                                          |              | No         |
| applyByNode                                                    |              | No         |
| areaBetween                                                    |              | No         |
| asPercent(seriesList, seriesList, nodeList) seriesList         |              | Stable     |
| averageAbove                                                   |              | No         |
| averageBelow                                                   |              | No         |
| averageOutsidePercentile                                       |              | No         |
| averageSeries(seriesLists) series                              | avg          | Stable     |
| averageSeriesWithWildcards                                     |              | No         |
| cactiStyle                                                     |              | No         |
| changed                                                        |              | No         |
| color                                                          |              | No         |
| consolidateBy(seriesList, func) seriesList                     |              | Stable     |
| constantLine                                                   |              | No         |
| countSeries(seriesLists) series                                |              | Stable     |
| cumulative                                                     |              | No         |
| currentAbove                                                   |              | No         |
| currentBelow                                                   |              | No         |
| dashed                                                         |              | No         |
| delay                                                          |              | No         |
| derivative(seriesLists) series                                 |              | Stable     |
| diffSeries(seriesLists) series                                 |              | Stable     |
| divideSeries(dividend, divisor) seriesList                     |              | Stable     |
| divideSeriesLists(dividends, divisors) seriesList              |              | Stable     |
| drawAsInfinite                                                 |              | No         |
| events                                                         |              | No         |
| exclude(seriesList, pattern) seriesList                        |              | Stable     |
| exponentialMovingAverage                                       |              | No         |
| fallbackSeries                                                 |              | No         |
| filterSeries(seriesList, func, operator, threshold) seriesList |              | Stable     |
| grep(seriesList, pattern) seriesList                           |              | Stable     |
| group                                                          |              | No         |
| groupByNode                                                    |              | No         |
| groupByNodes                                                   |              | No         |
| groupByTags(seriesList, func, tagList) seriesList              |              | Stable     |
| highest(seriesList, n, func) seriesList                        |              | Stable     |
| highestAverage(seriesList, n, func) seriesList                 |              | Stable     |
| highestCurrent(seriesList, n, func) seriesList                 |              | Stable     |
| highestMax(seriesList, n, func) seriesList                     |              | Stable     |
| hitcount                                                       |              | No         |
| holtWintersAberration                                          |              | No         |
| holtWintersConfidenceArea                                      |              | No         |
| holtWintersConfidenceBands                                     |              | No         |
| holtWintersForecast                                            |              | No         |
| identity                                                       |              | No         |
| integral                                                       |              | No         |
| integralByInterval                                             |              | No         |
| interpolate                                                    |              | No         |
| invert                                                         |              | No         |
| isNonNull(seriesList) seriesList                               |              | Stable     |
| keepLastValue(seriesList, limit) seriesList                    |              | Stable     |
| legendValue                                                    |              | No         |
| limit                                                          |              | No         |
| linearRegression                                               |              | No         |
| lineWidth                                                      |              | No         |
| logarithm                                                      |              | No         |
| lowest(seriesList, n, func) seriesList                         |              | Stable     |
| lowestAverage(seriesList, n, func) seriesList                  |              | Stable     |
| lowestCurrent(seriesList, n, func) seriesList                  |              | Stable     |
| mapSeries                                                      | map          | No         |
| maximumAbove                                                   |              | No         |
| maximumBelow                                                   |              | No         |
| maxSeries(seriesList) series                                   | max          | Stable     |
| minimumAbove                                                   |              | No         |
| minimumBelow                                                   |              | No         |
| minMax                                                         |              | No         |
| minSeries(seriesList) series                                   | min          | Stable     |
| mostDeviant                                                    |              | No         |
| movingAverage(seriesLists, windowSize) seriesList              |              | Unstable   |
| movingMax                                                      |              | No         |
| movingMedian                                                   |              | No         |
| movingMin                                                      |              | No         |
| movingSum                                                      |              | No         |
| movingWindow                                                   |              | No         |
| multiplySeries(seriesList) series                              |              | Stable     |
| multiplySeriesWithWildcards                                    |              | No         |
| nonNegatievDerivative(seriesList, maxValue) seriesList         |              | Stable     |
| nPercentile                                                    |              | No         |
| offset                                                         |              | No         |
| offsetToZero                                                   |              | No         |
| percentileOfSeries                                             |              | No         |
| perSecond(seriesLists) seriesList                              |              | Stable     |
| pieAverage                                                     |              | No         |
| pieMaximum                                                     |              | No         |
| pieMinimum                                                     |              | No         |
| pow                                                            |              | No         |
| powSeries                                                      |              | No         |
| randomWalkFunction                                             | randomWalk   | No         |
| rangeOfSeries(seriesList) series                               |              | Stable     |
| reduceSeries                                                   | reduce       | No         |
| removeAbovePercentile                                          |              | No         |
| removeAboveValue(seriesList, n) seriesList                     |              | Stable     |
| removeBelowPercentile                                          |              | No         |
| removeBelowValue(seriesList, n) seriesList                     |              | Stable     |
| removeBetweenPercentile                                        |              | No         |
| removeEmptySeries                                              |              | No         |
| roundFunction                                                  |              | No         |
| scale(seriesList, num) series                                  |              | Stable     |
| scaleToSeconds(seriesList, seconds) seriesList                 |              | Stable     |
| secondYAxis                                                    |              | No         |
| seriesByTag                                                    |              | No         |
| setXFilesFactor                                                | xFilesFactor | No         |
| sinFunction                                                    | sin          | No         |
| smartSummarize                                                 |              | No         |
| sortBy(seriesList, func, reverse) seriesList                   |              | Stable     |
| sortByMaxima(seriesList) seriesList                            |              | Stable     |
| sortByMinima                                                   |              | No         |
| sortByName(seriesList, natural, reverse) seriesList            |              | Stable     |
| sortByTotal(seriesList) seriesList                             |              | Stable     |
| squareRoot                                                     |              | No         |
| stacked                                                        |              | No         |
| stddevSeries(seriesList) series                                |              | Stable     |
| stdev                                                          |              | No         |
| substr                                                         |              | No         |
| summarize(seriesList) seriesList                               |              | Stable     |
| sumSeries(seriesLists) series                                  | sum          | Stable     |
| sumSeriesWithWildcards                                         |              | No         |
| threshold                                                      |              | No         |
| timeFunction                                                   | time         | No         |
| timeShift                                                      |              | No         |
| timeSlice                                                      |              | No         |
| timeStack                                                      |              | No         |
| transformNull(seriesList, default=0) seriesList                |              | Stable     |
| unique                                                         |              | No         |
| useSeriesAbove                                                 |              | No         |
| verticalLine                                                   |              | No         |
| weightedAverage                                                |              | No         |
