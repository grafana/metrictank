# Graphite

For a general overview of how Metrictank relates and compares to Graphite, please see the [Readme](../README.md)

## Caveats

There are some small behavioral and functional differences with Graphite:

* Currently no support for rewriting old data; There is a reorder-buffer to support out-of-order writes to an extent.  Full archived data rewriting is on the roadmap.
* Will never move observations into the past (e.g. consolidation and rollups will only cause data to get an equal or higher timestamp)
* Graphite timezone defaults to Chicago, we default to server time
* xFilesfactor is currently not supported for rollups. It is fairly easy to address, but we haven't had a need for it yet.
* Graphite supports the following render formats: csv, json, dygraph, msgpack, pickle, png, pdf, raw, rickshaw, and svg.
  Metrictank only implements json, msgp, msgpack, and pickle. Grafana only uses json. In particular, Metrictank does not render images, because Grafana renders great.
* Some less commonly used functions are not implemented yet in Metrictank itself, but Metrictank can seamlessly proxy those to graphite-web (see below for details)
  At Grafana Labs, 90 to 95 % of requests get handled by Metrictank without involving Graphite.


## Processing functions

Metrictank aims to be able to provide as much processing power as it can: we're in the process
of implementing [Graphite's extensive processing api](http://graphite.readthedocs.io/en/latest/functions.html) into metrictank itself.

Below is an overview of all current Graphite functions (as of Aug 15, 2018) and their support in metrictank.
There are 3 levels of support:

* No : not implemented yet in metrictank or not applicable (e.g. graphite functions that affect graphical style but not json data)
* Stable : 100% compatible with graphite and vetted
* Unstable: not fully compatible yet or not vetted enough

When you request functions that Metrictank cannot provide, it will automatically, seamlessly proxy requests to graphite.
Those requests will not include response metadata, will still use Metrictank as a storage system if Graphite is configured that way, and may return a bit slower.
You can also choose to enable unstable functions via process=any
See also:
* [HTTP api docs for render endpoint](https://github.com/grafana/metrictank/blob/master/docs/http-api.md#graphite-query-api)
* [HTTP api configuration](https://github.com/grafana/metrictank/blob/master/docs/config.md#http-api).  Note the `fallback-graphite-addr` setting.


| Function name and signature                                    | Alias        | Metrictank |
| -------------------------------------------------------------- | ------------ | ---------- |
| absolute                                                       |              | Stable     |
| aggregate                                                      |              | Stable     |
| aggregateLine                                                  |              | No         |
| aggregateWithWildcards                                         |              | No         |
| alias(seriesList, alias) seriesList                            |              | Stable     |
| aliasByMetric                                                  |              | Stable     |
| aliasByNode(seriesList, nodeList) seriesList                   | aliasByTags  | Stable     |
| aliasByTags                                                    |              | No         |
| aliasQuery                                                     |              | No         |
| aliasSub(seriesList, pattern, replacement) seriesList          |              | Stable     |
| alpha                                                          |              | No         |
| applyByNode                                                    |              | No         |
| areaBetween                                                    |              | No         |
| asPercent(seriesList, seriesList, nodeList) seriesList         |              | Stable     |
| averageAbove                                                   |              | Stable     |
| averageBelow                                                   |              | Stable     |
| averageOutsidePercentile                                       |              | No         |
| averageSeries(seriesLists) series                              | avg          | Stable     |
| averageSeriesWithWildcards                                     |              | No         |
| cactiStyle                                                     |              | No         |
| changed                                                        |              | No         |
| color                                                          |              | No         |
| consolidateBy(seriesList, func) seriesList                     |              | Stable     |
| constantLine                                                   |              | No         |
| countSeries(seriesLists) series                                |              | Stable     |
| cumulative                                                     |              | Stable     |
| currentAbove                                                   |              | Stable     |
| currentBelow                                                   |              | Stable     |
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
| fallbackSeries                                                 |              | Stable     |
| filterSeries(seriesList, func, operator, threshold) seriesList |              | Stable     |
| grep(seriesList, pattern) seriesList                           |              | Stable     |
| group                                                          |              | Stable     |
| groupByNode                                                    |              | Stable     |
| groupByNodes                                                   |              | Stable     |
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
| integral                                                       |              | Stable     |
| integralByInterval                                             |              | No         |
| interpolate                                                    |              | No         |
| invert                                                         |              | Stable     |
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
| maximumAbove                                                   |              | Stable     |
| maximumBelow                                                   |              | Stable     |
| maxSeries(seriesList) series                                   | max          | Stable     |
| minimumAbove                                                   |              | Stable     |
| minimumBelow                                                   |              | Stable     |
| minMax                                                         |              | Stable     |
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
| removeAbovePercentile(seriesList, n) seriesList                |              | No         |
| removeAboveValue(seriesList, n) seriesList                     |              | Stable     |
| removeBelowPercentile(seriesList, n) seriesList                |              | No         |
| removeBelowValue(seriesList, n) seriesList                     |              | Stable     |
| removeBetweenPercentile                                        |              | No         |
| removeEmptySeries                                              |              | Stable     |
| round                                                          |              | Stable     |
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
| unique                                                         |              | Stable     |
| useSeriesAbove                                                 |              | No         |
| verticalLine                                                   |              | No         |
| weightedAverage                                                |              | No         |
