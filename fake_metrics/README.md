```
./fake_metrics -h
Usage of ./fake_metrics:
  -carbon-tcp-address string
    	carbon TCP address. e.g. localhost:2003
  -flushPeriod int
    	period in ms between flushes. metricPeriod must be cleanly divisible by flushPeriod. does not affect volume/throughput per se. the message is adjusted as to keep the volume/throughput constant (default 100)
  -kafka-tcp-address string
    	kafka TCP address. e.g. localhost:9092
  -keys-per-org int
    	how many metrics per orgs to simulate (default 100)
  -log-level int
    	log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL (default 2)
  -metricPeriod int
    	period in seconds between metric points (default 1)
  -nsqd-tcp-address string
    	nsqd TCP address. e.g. localhost:4150
  -offset string
    	offset duration expression. (how far back in time to start. e.g. 1month, 6h, etc
  -orgs int
    	how many orgs to simulate (default 2000)
  -speedup int
    	for each advancement of real time, how many advancements of fake data to simulate (default 1)
  -statsd-addr string
    	statsd address (default "localhost:8125")
  -statsd-type string
    	statsd type: standard or datadog (default "standard")
  -stop-at-now
    	stop program instead of starting to write data with future timestamps
  -topic string
    	NSQ topic (default "metrics")
  -version
    	print version string
```

# there's 2 main use cases to run this tool:

mimic a real-time load of 40k metrics/s:

```
./fake_metrics -keys-per-org 100 -orgs 400 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150
```

or perform a backfill of data, in this example a years worth of data at a 40k x speedup, which takes about 13min.
obviously you need to use fewer metrics to do this.

```
./fake_metrics -keys-per-org 1 -orgs 1 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150 -offset 1y -speedup 40000
```


or a workload where you want the last days' worth of data to be filled but you also want a good amount of total metrics,
this gives a realistic workload for testing GC and such:

```
./fake_metrics -keys-per-org 100 -orgs 10 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150 -offset 1d -speedup 100 -stop-at-now
./fake_metrics -keys-per-org 100 -orgs 10 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150
```

# Outputs

NSQ and kafka are multi-tenant outputs where structured data is sent and multiple orgs may have the same key in their own namespace.
carbon and gnet are single-tenant.
so in that case you can only simulate one org otherwise the keys would overwrite each other.
for the gnet output, the org-id will be set to whatever you authenticate as (unless you use the admin key),

# Important

we use ticker based loops in which we increment timestamps and call output Flush methods.
if a loop iteration takes too long (due to an output's Flush taking too long for example),
ticks will be missed and the data will start lagging behind.
So make sure your flushInterval is large enough to account for how long the flushing of each
output may take.  The Gnet output helps a little by decoupling publishing from the Flush() call with a queue.
If the queue runs full, or if any output's Flush() takes too long, ticks will be skipped, dropping throughput and data will lag behind.

Keep an eye on the flush (and publish) durations of your outputs and the queue size if applicable.
see included dashboard.

Note: the publishing metrics represent the step of writing data to the backend service.
If message serialization/data formatting is a separate step, then it is not included in the publish time
(but it is included in the flush time)
