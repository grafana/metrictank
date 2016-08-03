# operations

## monitoring

You should monitor the dependencies according to their best practices.
In particular, pay attention to delays in your kafka queue, if you use it.
Especially for metric persistence messages: if those have issues, chunks may be saved multiple times
when you move around the primary role.

Metrictank uses statsd to report metrics about itself. See [the list of documented metrics](https://github.com/raintank/metrictank/blob/master/docs/metrics.md)

### dashboard

grafana.net


### useful metrics to monitor/alert on

* process is running and listening on its http port (and carbon port, if you enabled it) (use your monitoring agent of choice for this)
* `stats.*.gauges.metric_tank.*.cluster.primary`: assures you have exactly 1 primary node (saving to cassandra)
* `stats.*.timers.metric_tank.*.cassandra.write_queue.*.items.upper`: make sure the write queues are able to drain and don't reach capacity, otherwise ingest will block
* `stats.$environment.timers.metrictank.*.es_put_duration.{mean,upper*}`: if it takes too long to index to ES, ingest will start blocking
* `stats.$environment.timers.metrictank.$host.request_handle_duration.median`: shows how fast/slow MT responds to http queries

If you expect consistent or predictable load, you may also want to monitor:

* `stats.*.metric_tank.*.chunks.save_ok`: amount of saved chunks (based on your chunkspan settings)
* `stats.*.timers.metric_tank.*.requests_span.*.count_ps` : volume of requests MT receives



## primary failover


