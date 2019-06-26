# Frequently asked questions

## Does it work well with transient data, e.g. new metrics constantly appearing and other metrics becoming inactive, such as those from docker containers ?

For any metric, we only store data that is received, as well as an entry into the index.  So the overhead of a shortlived metric is low.

Furthermore, we have optimizations for this use case:

* Index filtering: when you request data, we exclude items from the result set that have not been updated in 24hours before the "from" of the request (as the data will be all null anyway)
* Index pruning: if enabled, we delete series from the index if no data has been received in "max-stale" time. (but keep data until it expires, in case the same metric gets re-added). This is useful because the query editor does not send a time range. Note that this setting is applied to *all* metrics.
* GC: removes metrics from metrictank's ring buffer if they become stale (see `metric-max-stale`), which means data will most likely come from cassandra or possibly the in-memory chunk-cache, but does not affect the index.

## What happens when I want to update the resolution / interval of a metric?

Update the interval tag sent with your data (if using [carbon-relay-ng](https://github.com/graphite-ng/carbon-relay-ng) update your storage-schemas.conf) and send data at the new resolution.
Metrictank will automatically combine the data together like so:
* when a response contains a series for which there is data of multiple resolutions, the higher resolution data will be consolidated down to match the lower resolution.
* when a response contains multiple series of different resolutions, the higher resolution data will be consolidated to the "lowest common denominator" resolution, so the output has a consistent interval.
* when a response contains multiple series of different resolutions, of which at least one series has data of different resolutions, the two mechanisms above work together.
* once a request can be fully satistfied by data with the new resolution, that's the resolution the response will be in.

## Tag support

Metrics can have tags and metrictank supports queries via the [graphite tag query api](https://graphite.readthedocs.io/en/latest/tags.html)
In the future, metrictank aims to provide a proper implementation of [metrics 2.0](http://metrics20.org/)
(it helps that both projects share the same main author)

## What are different ways to reason about "active metrics"?

* `tank.metrics_active`: number of series known to the tank (i.o.w. for which we have in-memory buffers). 
  each metricdefinition corresponds to one series, so this excludes rollups.
  series get cleared every gc-interval (1h by default) by comparing metric-max-stale (3h by default) against the lastWrite property,
  which is wallclock time of when last point was successfully added
* `index.metrics_active`: the number of metricdefinitions held within the in-memory index (i.o.w. that are available for querying)
  subject to pruning via the [index rules config file](https://github.com/grafana/metrictank/blob/master/docs/config.md#index-rulesconf)
  (these rules are applied during index loading at startup time and at runtime during Prune(), see prune-interval setting for your index plugin. 3h by default)
   (compared against lastUpdate property, which is set to timestamp of most recent data)
* output of `mt-index-cat`:
  queries the persistent index using a single "default" index rule based on the maxStale flag (exclude entries with lastUpdate too old)
  and cuts off on the fly based on minStale (which excludes entries that have a lastUpdate "too recent". disabled by default)
  So this is equivalent to the index, assuming the index prune-interval is sufficiently low and assuming you don't filter via minStale (which the index doesn't do)
