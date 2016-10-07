# Operations

## Monitoring

You should monitor the dependencies according to their best practices.
In particular, pay attention to delays in your kafka queue, if you use it.
Especially for metric persistence messages which flow from primary to secondary nodes: if those have issues, chunks may be saved multiple times
when you move around the primary role. (see [clustering transport](https://github.com/raintank/metrictank/blob/master/docs/clustering.md))

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



## Crash


Metrictank crashed. What to do?

### Diagnosis

1) Check `dmesg` to see if it was killed by the kernel, maybe it was consuming too much RAM
   If it was, check the grafana dashboard which may explain why. (sudden increase in ingested data? increase in requests or the amount of data requested? slow requests?)
   Tips:
   * The [profiletrigger](https://github.com/raintank/metrictank/blob/master/docs/config.md#profiling-instrumentation-and-logging) functionality can automatically trigger
   a memory profile and save it to disk.  This can be very helpful if suddently memory usage spikes up and then metrictank gets killed in seconds or minutes.  
   It helps diagnose problems in the codebase that may lead to memory savings.  The profiletrigger looks at the `bytes_sys` metric which is
   the amount of memory consumed by the process.
   * Use [rollups](https://github.com/raintank/metrictank/blob/master/docs/consolidation.md#rollups) to be able to answer queries for long timeframes with less data
2) Check the metrictank log.
   If it exited due to a panic, you should probably open a [ticket](https://github.com/raintank/metrictank/issues) with the output of `metrictank --version`, the panic, and perhaps preceeding log data.
   If it exited due to an error, it could be a problem in your infrastructure or a problem in the metrictank code (in the latter case, please open a ticket as described above)

### Recovery

#### If you run multiple instances

* If the crashed instance was a secondary, you can just restart it and after it warmed up, it will ready to serve requests.  Verify that you have other instances who can serve requests, otherwise you may want to start it with a much shorter warm up time.  It will be ready to serve requests sooner, but may have to reach out to Cassandra more to load data.
* If the crashed instance was a primary, you have to bring up a new primary.  Based on when the primary was able to last save chunks, and how much data you keep in RAM (using [chunkspan * numchunks](https://github.com/raintank/metrictank/blob/master/docs/data-knobs.md#basic-guideline), you can calculate how quickly you need to promote an already running secondary to primary to avaid dataloss.  If you don't have a secondary up long enough, pick whichever was up the longest.  


#### If you only run once instance

If you use the kafka-mdm input (at raintank we do), before restarting check your [offset option](https://github.com/raintank/metrictank/blob/master/docs/config.md#kafka-mdm-input-optional-recommended).   Most of our customers who run a single instance seem to prefer the `last` option: preferring immidiately getting realtime insights back, at the cost of missing older data.


## Metrictank hangs

if the metrictank process seems "stuck".. not doing anything, but up and running, you can report a bug.
Please include the following information:

* stacktrace obtained with `curl 'http://<ip>:<port>/debug/pprof/goroutine?debug=2'`
* cpu profile obtained with `curl 'http://<ip>:<port>/debug/pprof/profile'`
* output of `metrictank --version`.


## Primary failover

* stop the primary: `curl -X POST -d primary=false http://<node>:6060/cluster`
* make sure it's finished saving metricpersist messages (see its dashboard)
* promote the candidate to primary: `curl -X POST -d primary=true http://<node>:6060/cluster`
* you can verify the cluster status through `curl http://<node>:6060/cluster` or on the Grafana dashboard (see above)

For more information see [Clustering: Promoting a secondary to primary](https://github.com/raintank/metrictank/blob/master/docs/clustering.md#promoting-a-secondary-to-primary)

See [HTTP api docs](https://github.com/raintank/metrictank/blob/master/docs/http-api.md)

## Ingestion stalls & backpressure

If metrictank ingestion speed is lower than expected, or decreased for seemingly no reason, it may be due to:

1) [Indexing of metadata](https://github.com/raintank/metrictank/blob/master/docs/metadata.md) puts backpressure on the ingest stream.   
   New metrics (including metrics with new settings such as interval, unit, or tags) need to get indexed into:
   * an internal index (which seems to always be snappy and not exert any backpressure)
   * Cassandra or Elasticsearch, which tends to not keep up with throughput, resulting in backpressure, and a lowered ingestion rate.
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
