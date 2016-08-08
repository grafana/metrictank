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
* `stats.$environment.timers.metrictank.$host.request_handle_duration.median`: shows how fast/slow MT responds to http queries

If you expect consistent or predictable load, you may also want to monitor:

* `stats.*.metric_tank.*.chunks.save_ok`: amount of saved chunks (based on your chunkspan settings)
* `stats.*.timers.metric_tank.*.requests_span.*.count_ps` : volume of requests MT receives



## primary failover

* stop the primary: `curl -X POST -d primary=false http://<node>:6060/cluster`
* make sure it's finished saving metricpersist messages (see its dashboard)
* pick a candidate secondary to promote. (any node that has `promotion wait` value in its dashboard as 0, or as low as possible). It needs to run for a while, based on your chunkspan settings
* promote the node to primary: `curl -X POST -d primary=true http://<node>:6060/cluster`
* you can verify the cluster status through `curl http://<node>:6060/cluster` or on the Grafana dashboard (see above)

See [HTTP api docs](https://github.com/raintank/metrictank/blob/master/docs/http-api.md)

