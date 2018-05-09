# Operations

## Monitoring

You should monitor the dependencies according to their best practices.
In particular, pay attention to delays in your kafka queue, if you use it.
Especially for metric persistence messages which flow from primary to secondary nodes: if those have issues, chunks may be saved multiple times
when new primaries come online (or get promoted). (see [clustering transport](https://github.com/grafana/metrictank/blob/master/docs/clustering.md))

Metrictank reports metrics about itself. See [the list of documented metrics](https://github.com/grafana/metrictank/blob/master/docs/metrics.md)

### Dashboard

You can import the [Metrictank dashboard from Grafana.net](https://grafana.net/dashboards/279) into your Grafana.
this will give instant insights in all the performance metrics of Metrictank.


### Useful metrics to monitor/alert on

* process is running and listening on its http port (and carbon port, if you enabled it) (use your monitoring agent of choice for this)
* `metrictank.stats.$environment.$instance.cluster.primary.gauge1`: assure you have exactly 1 primary node (saving to cassandra) or as many as you have shardgroups, for sharded setups.
* `metrictank.stats.$environment.$instance.input.kafka-mdm.partition.*.lag.gauge64`: kafka lag, depending on your throughput you can always expect some lag, but it should be in the thousands not millions.
* `metrictank.stats.$environment.$instance.store.cassandra.write_queue.*.items.{min,max}.gauge32`: make sure the write queues are able to drain.  For primary nodes that are also used for qureies, assert the write queues don't reach capacity, otherwise ingest will block and data will lag behind in queries.
* `metrictank.stats.$environment.$instance.input.*.metricpoint.unknown.counter32`: counter of MetricPoint messages for an unknown metric, will be dropped.
* `metrictank.stats.$environment.$instance.input.*.*.invalid.counter32`: counter of incoming data that could not be decoded.
* `metrictank.stats.$environment.$instance.tank.metrics_too_old.counter32`: counter of points that are too old and can't be added.
* `metrictank.stats.$environment.$instance.api.request_handle.latency.*.gauge32`: shows how fast/slow metrictank responds to http queries
* `metrictank.stats.$environment.$instance.store.cassandra.error.*`: shows erroring queries.  Queries that result in errors (or timeouts) will result in missing data in your charts.
* `perSecond(metrictank.stats.$environment.$instance.tank.add_to_closed_chunk.counter32)`: Points dropped due to chunks being closed. Need to tune the chunk-max-stale setting or fix your data stream to not send old points so late.
* `metrictank.stats.$environment.$instance.recovered_errors.*.*.*` : any internal errors that were recovered from automatically (should be 0. If not, please create an issue)

If you expect consistent or predictable load, you may also want to monitor:

* `metrictank.stats.$environment.$instance.store.cassandra.chunk_operations.save_ok.counter32`: number of saved chunks (based on your chunkspan settings)
* `metrictank.stats.$environment.$instance.api.request_handle.values.rate32` : rate per second of render requests
* `metrictank.stats.$environment.$instance.input.*.*.received.counter32`: input counter (derive with perSecond(


## Crash


Metrictank crashed. What to do?

### Diagnosis

1) Check `dmesg` to see if it was killed by the kernel, maybe it was consuming too much RAM
   If it was, check the grafana dashboard which may explain why. (sudden increase in ingested data? increase in requests or the amount of data requested? slow requests?)
   Tips:
   * The [profiletrigger](https://github.com/grafana/metrictank/blob/master/docs/config.md#profiling-instrumentation-and-logging) functionality can automatically trigger
   a memory profile and save it to disk.  This can be very helpful if suddently memory usage spikes up and then metrictank gets killed in seconds or minutes.  
   It helps diagnose problems in the codebase that may lead to memory savings.  The profiletrigger looks at the `bytes_sys` metric which is
   the amount of memory consumed by the process.
   * Use [rollups](https://github.com/grafana/metrictank/blob/master/docs/consolidation.md#rollups) to be able to answer queries for long timeframes with less data
2) Check the metrictank log.
   If it exited due to a panic, you should probably open a [ticket](https://github.com/grafana/metrictank/issues) with the output of `metrictank --version`, the panic, and perhaps preceding log data.
   If it exited due to an error, it could be a problem in your infrastructure or a problem in the metrictank code (in the latter case, please open a ticket as described above)

### Recovery

#### If you run multiple instances

* If the crashed instance was a secondary, you can just restart it and after it warmed up (or backfilled data from Kafka), it will ready to serve requests.  Verify that you have other instances who can serve requests, otherwise you may want to start it with a much shorter warm up time.  It will be ready to serve requests sooner, but may have to reach out to Cassandra more to load data.
* If the crashed instance was a primary, you have to bring up a new primary.  Based on when the primary was able to last save chunks, and how much data you keep in RAM (using [chunkspan * numchunks](https://github.com/grafana/metrictank/blob/master/docs/memory-server.md), you can calculate how quickly you need to promote an already running secondary to primary to avaid dataloss.  If you don't have a secondary up long enough, pick whichever was up the longest.  


#### If you only run once instance

If you use the kafka-mdm input (at grafana we do), before restarting check your [offset option](https://github.com/grafana/metrictank/blob/master/docs/config.md#kafka-mdm-input-optional-recommended).   Most of our customers who run a single instance seem to prefer the `last` option: preferring immediately getting realtime insights back, at the cost of missing older data.


## Metrictank hangs

if the metrictank process seems "stuck".. not doing anything, but up and running, you can report a bug.
Please include the following information:

* stacktrace obtained with `curl 'http://<ip>:<port>/debug/pprof/goroutine?debug=2'`
* cpu profile obtained with `curl 'http://<ip>:<port>/debug/pprof/profile'`
* output of `metrictank --version`.

You can also send `SIGQUIT` via Control-Backslash to the process, in which case it will print the stack dump and then exit.
See [behavior of signals in Go programs](https://golang.org/pkg/os/signal/#hdr-Default_behavior_of_signals_in_Go_programs) for more information.

If you're feeling lucky or are a developer, you can also get a trace like so:
`curl 'http://<ip>:<port>/debug/pprof/trace?seconds=20' --output trace.data`


## Primary failover

* stop the primary: `curl -X POST -d primary=false http://<node>:6060/node`
* make sure it's finished saving metricpersist messages (see its dashboard)
* promote the candidate to primary: `curl -X POST -d primary=true http://<node>:6060/node`
* you can verify the cluster status through `curl http://<node>:6060/node` or on the Grafana dashboard (see above)

For more information see [Clustering: Promoting a secondary to primary](https://github.com/grafana/metrictank/blob/master/docs/clustering.md#promoting-a-secondary-to-primary)

See [HTTP api docs](https://github.com/grafana/metrictank/blob/master/docs/http-api.md)

## Ingestion stalls & backpressure

If metrictank ingestion speed is lower than expected, or decreased for seemingly no reason, it may be due to:

1) [Indexing of metadata](https://github.com/grafana/metrictank/blob/master/docs/metadata.md) puts backpressure on the ingest stream.   
   New metrics (including metrics with new settings such as interval, unit, or tags) need to get indexed into:
   * the in-memory index (which generally should not exert backpressure)
   * Cassandra (index) - if enabled - which may not keep up with throughput, resulting in backpressure, and a lowered ingestion rate.
   Check the index stats on the dashboard.

2) Saving of chunks.  Metrictank saves chunks at the rhythm of your [chunkspan](https://github.com/grafana/metrictank/blob/master/docs/memory-server.md) (10 minutes in the default docker image)
   When this happens, it will need to save a bunch of chunks and
   [based on the configuration of your write queues and how many series you have](https://github.com/grafana/metrictank/issues/125) the queues may run full and
   provide ingestion backpressure, also lowering ingestion speed.  
   Store (cassandra) saving backpressure is also visualized on the 'metrics in' graph of the dashboard.
   Additionally, the 'write workers & queues' graph shows the queue limit and how many items are in the queues.   
   The queues are drained by saving chunks, but populated by new chunks that need to be saved.  Backpressure is active is when the queues are full (when number of items equals the limit).
   It's possible for the queues to stay at the limit for a while, despite chunks being saved (when there's new chunks that also need to be saved).
   However you should probably tune your queue sizes in this case.  See [our Cassandra page](https://github.com/grafana/metrictank/blob/master/docs/cassandra.md)
   Of course, if metrictank is running near peak capacity, The added workload of saving data may also lower ingest speed.

3) golang GC runs may cause ingest drops.  Look at 'golang GC' in the Grafana dashboard and see if you can get the dashboard zoom right to look at individual GC runs, and see if they correspond to the ingest drops. (shared cursor is really handy here)

4) doing http requests to metrictank can lower its ingestion performance. (note that the dashboard in the docker stack loads
from metrictank as well). normally we're talking about hundreds of requests (or very large ones) where you can start to see this effect, but the effect also becomes more apparent with large ingest rates where metrictank gets closer to saturation

## Metrictank uses too much memory

If metrictank consumes too much memory and you want to know why. There are a few usual suspects:
* the ringbuffers, especially if you have high values for numchunks, chunkspan, especially for rollup archives that use many aggregation functions.
* chunk-cache (see your max-size setting)
* metadata index, especially if you have many series.

To get insights into memory usage:
1) use the grafana dashboard.  If memory grows significantly at a given point, figure out what happened at that point (were new metrics ingested into the system?)
2) use the profiletrigger: this automatically collects profiles when memory usage reaches a certain point (see `proftrigger-*` settings)
3) if you want to understand memory usage "right now", you can take a live profile. This is fairly easy and only requires you have the `go` tool installed.
   It has a very low, usually insignificant resource impact. Unless you have set up a low, non-default value for `mem-profile-rate` in your config. 

Taking a profile:
```
wget http://localhost:6060/debug/pprof/heap
go tool pprof <path-to-binary> heap
```
It is very important that you use the correct metrictank binary. It should not be a different version.

The `go tool pprof` command will give you a command prompt.  
* type `top30` to get a good idea of where memory is spent. The first columns (`flat` and `flat%`) show memory used by just that code. The last colum (`cum` and `cum%` show memory used by that code and everything called from it. (which is why certain things like the input plugins use a lot of memory cumulatively, because all data being added to the memory cache originates from that code path)
* type `web` to see a visual representation in the browser, which can show how code calls each other
* type `list <regex matching type and function name>` to see a breakdown of lines of code of a function, along with the memory allocated by each line. (e.g. `list chunk.New`)
Type exit (or ctrl-D) to exit the prompt.
For more information on profiling see the excellent [Profiling Go Programs](https://blog.golang.org/profiling-go-programs) article.


## data doesn't show up

* make sure you specify a correct interval. sending minutely data with interval specified as 10s will result in 5 nulls for each point, which in combination with certain grafana display settings ("like nulls connected") may not show anything
* make sure consumption from input works fine and is not lagging (see dashboard)
* check if any points are being rejected, using the ingest chart on the dashboard (e.g. out of order, invalid)
* can use debug logging to trace data throughout the pipeline. mt-store-cat to see what's in cassandra, mt-kafka-mdm-sniff, etc.
* if it's old data, make sure you have a primary that can save data to cassandra, that the write queue can drain
* check `metric-max-stale` and `chunk-max-stale` settings, make sure chunks are not being prematurely sealed (happens in some rare cases if you send data very infrequently. see `tank.add_to_closed_chunk` metric)

## Opentracing

Metrictank supports opentracing via [Jaeger](http://jaeger.readthedocs.io/en/latest/)
It can give good insights into why certain requests are slow, and is easy to run.
To use, enable in the config and point it at a Jaeger collector.
