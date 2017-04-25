# Graphite

Metrictank aims to be able to provide as much processing power as it can: we're in the process
of implementing [Graphite's extensive processing api](http://graphite.readthedocs.io/en/latest/functions.html) into metrictank itself.
We're only just getting started, which is why metrictank will automatically proxy requests to graphite if functions are requested
that it cannot provide. You can also choose to enable unstable functions via process=any
See also:
* [HTTP api docs for render endpoint](https://github.com/raintank/metrictank/blob/master/docs/http-api.md#graphite-query-api)
* [HTTP api configuration](https://github.com/raintank/metrictank/blob/master/docs/config.md#http-api).  Note the `fallback-graphite-addr` setting.

Here are the currently included functions:

Function name and signature                          | Alias        | Metrictank
---------------------------------------------------- | ------------ | ----------
alias(seriesList, alias) seriesList                  |              | Stable
averageSeries(seriesLists) series                    | avg          | Stable
consolidateBy(seriesList, func) seriesList           |              | Stable
movingAverage(seriesLists, windowSize) seriesList    |              | Unstable
sumSeries(seriesLists) series                        | sum          | Stable
