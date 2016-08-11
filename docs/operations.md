# operations

## monitoring

You should monitor the dependencies according to their best practices.
In particular, pay attention to delays in your kafka queue, if you use it.
Especially for metric persistence messages: if those have issues, chunks may be saved multiple times
when you move around the primary role.

Metrictank uses statsd to report metrics about itself. See [the list of documented metrics](https://github.com/raintank/metrictank/blob/master/docs/metrics.md)

### dashboard

You can import the [Metrictank dashboard from Grafana.net](https://grafana.net/dashboards/279) into your Grafana.
this will give instant insights in all the performance metrics of Metrictank.
Just make sure to have a properly configured statsd setup (or adjust the dashboard)


### useful metrics to monitor/alert on

* process is running and listening on its http port (and carbon port, if you enabled it) (use your monitoring agent of choice for this)
* `stats.*.gauges.metric_tank.*.cluster.primary`: assure you have exactly 1 primary node (saving to cassandra)
* `stats.*.timers.metric_tank.*.cassandra.write_queue.*.items.upper`: make sure the write queues are able to drain and don't reach capacity, otherwise ingest will block
* `stats.$environment.timers.metrictank.*.es_put_duration.{mean,upper*}`: if it takes too long to index to ES, ingest will start blocking
* `stats.$environment.timers.metrictank.$host.request_handle_duration.median`: shows how fast/slow metrictank responds to http queries

If you expect consistent or predictable load, you may also want to monitor:

* `stats.*.metric_tank.*.chunks.save_ok`: amount of saved chunks (based on your chunkspan settings)
* `stats.*.timers.metric_tank.*.requests_span.*.count_ps` : volume of requests metrictank receives



## primary failover

* stop the primary: `curl -X POST -d primary=false http://<node>:6060/cluster`
* make sure it's finished saving metricpersist messages (see its dashboard)
* pick a candidate secondary to promote. (any node that has `promotion wait` value in its dashboard as 0, or as low as possible). It needs to run for a while, based on your chunkspan settings
* promote the node to primary: `curl -X POST -d primary=true http://<node>:6060/cluster`
* you can verify the cluster status through `curl http://<node>:6060/cluster` or on the Grafana dashboard (see above)

See [HTTP api docs](https://github.com/raintank/metrictank/blob/master/docs/http-api.md)

## ingestion stalls & backpressure

If metrictank ingestion speed is lower than expected, or decreased for seemingly no reason, it may be due to:

1) [Indexing of metadata](https://github.com/raintank/metrictank/blob/master/docs/metadata.md) - currently using ElasticSearch - puts backpressure on the ingest stream.   
   New metrics (including metrics with new settings such as interval, unit, or tags) need to get indexed.  If ES can't keep up, the ingest rate will decrease.  
   You can tell by looking at the 'ES index writes' chart in the dashboard to see if there's indexing activity and whether it lines up with the ingest decrease.
   ingest will go faster again once metrics have been indexed.
2) Saving of chunks.  Metrictank saves chunks at the rhythm of your [chunkspan](https://github.com/raintank/metrictank/blob/master/docs/data-knobs.md) (10 minutes in the default docker image)
   When this happens, it will need to save a bunch of chunks and
   [based on the configuration of your write queues and how many series you have](https://github.com/raintank/metrictank/issues/125) the queues may run full and
   provide ingestion backpressure, also lowering ingestion speed.  
   You can easily spot this on the dashboard when the write queues jump up in size and stay at the same size, despite chunks getting saved.  If this lines up with the ingestion drop, you have your culprit.  Of course, if metrictank is running near peak capacity, The added workload of saving data may also lower ingest speed.
3) golang GC runs may cause ingest drops.  Look at 'golang GC' in the Grafana dashboard and see if you can get the dashboard zoom right to look at individual GC runs, and see if they correspond to the ingest drops. (shared cursor is really handy here)
4) doing http requests to metrictank can lower its ingestion performance. (note that the dashboard in the docker stack loads from metrictank as well). normally we're talking about hundreds of requests (or very large ones) where you can start to see this effect, but the effect also becomes more apparant with large ingest rates where metrictank gets closer to saturation
