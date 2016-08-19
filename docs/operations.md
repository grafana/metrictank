# Operations

## Monitoring

You should monitor the dependencies according to their best practices.
In particular, pay attention to delays in your kafka queue, if you use it.
Especially for metric persistence messages: if those have issues, chunks may be saved multiple times
when you move around the primary role.

Metrictank uses statsd to report metrics about itself. See [the list of documented metrics](https://github.com/raintank/metrictank/blob/master/docs/metrics.md)

### Dashboard

You can import the [Metrictank dashboard from Grafana.net](https://grafana.net/dashboards/279) into your Grafana.
this will give instant insights in all the performance metrics of Metrictank.
Just make sure to have a properly configured statsd setup (or adjust the dashboard)


### Useful metrics to monitor/alert on

* process is running and listening on its http port (and carbon port, if you enabled it) (use your monitoring agent of choice for this)
* `stats.*.gauges.metric_tank.*.cluster.primary`: assure you have exactly 1 primary node (saving to cassandra)
* `stats.*.timers.metric_tank.*.cassandra.write_queue.*.items.upper`: make sure the write queues are able to drain and don't reach capacity, otherwise ingest will block
* `stats.$environment.timers.metrictank.*.es_put_duration.{mean,upper*}`: if it takes too long to index to ES, ingest will start blocking
* `stats.$environment.timers.metrictank.$host.request_handle_duration.median`: shows how fast/slow metrictank responds to http queries

If you expect consistent or predictable load, you may also want to monitor:

* `stats.*.metric_tank.*.chunks.save_ok`: amount of saved chunks (based on your chunkspan settings)
* `stats.*.timers.metric_tank.*.requests_span.*.count_ps` : volume of requests metrictank receives



## Primary failover

* stop the primary: `curl -X POST -d primary=false http://<node>:6060/cluster`
* make sure it's finished saving metricpersist messages (see its dashboard)
* pick a candidate secondary to promote. (any node that has `promotion wait` value in its dashboard as 0, or as low as possible). It needs to run for a while, based on your chunkspan settings
* promote the node to primary: `curl -X POST -d primary=true http://<node>:6060/cluster`
* you can verify the cluster status through `curl http://<node>:6060/cluster` or on the Grafana dashboard (see above)

See [HTTP api docs](https://github.com/raintank/metrictank/blob/master/docs/http-api.md)

## Ingestion stalls & backpressure

If metrictank ingestion speed is lower than expected, or decreased for seemingly no reason, it may be due to:

1) [Indexing of metadata](https://github.com/raintank/metrictank/blob/master/docs/metadata.md) puts backpressure on the ingest stream.   
   New metrics (including metrics with new settings such as interval, unit, or tags) need to get indexed into:
   * an internal index (which seems to always be snappy and not exert any backpressure)
   * Elasticsearch, which tends to not keep up with throughput, resulting in backpressure, and a lowered ingestion rate.
   ES backpressure is visualized in the 'metrics in' graph of the metrictank dashboard.
   For more details, look at the 'ES index writes' chart in the dashboard, specifically latency timings and adding to bulkindexer activity, those create the backpressure.

2) Saving of chunks.  Metrictank saves chunks at the rhythm of your [chunkspan](https://github.com/raintank/metrictank/blob/master/docs/data-knobs.md) (10 minutes in the default docker image)
   When this happens, it will need to save a bunch of chunks and
   [based on the configuration of your write queues and how many series you have](https://github.com/raintank/metrictank/issues/125) the queues may run full and
   provide ingestion backpressure, also lowering ingestion speed.  
   Store (cassandra) saving backpressure is also visualized on the 'metrics in' graph of the dashboard.
   Additionally, the 'write workers & queues' graph shows the queue limit and how many items are in the queues.   
   The queues are drained by saving chunks, but populated by new chunks that need to be saved.  Backpressure is active is when the queues are full (when number of items equals the limit).
   It's possible for the queues to stay at the limit for a while, despite chunks being saved (when there's new chunks that also need to be saved).
   However you should probably tune your queue sizes in this case.  See [our Cassandra page](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md)
   Of course, if metrictank is running near peak capacity, The added workload of saving data may also lower ingest speed.

3) golang GC runs may cause ingest drops.  Look at 'golang GC' in the Grafana dashboard and see if you can get the dashboard zoom right to look at individual GC runs, and see if they correspond to the ingest drops. (shared cursor is really handy here)

4) doing http requests to metrictank can lower its ingestion performance. (note that the dashboard in the docker stack loads
from metrictank as well). normally we're talking about hundreds of requests (or very large ones) where you can start to see this effect, but the effect also becomes more apparant with large ingest rates where metrictank gets closer to saturation
