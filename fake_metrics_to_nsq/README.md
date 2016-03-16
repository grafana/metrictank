
```
Usage of ./fake_metrics_to_nsq:
  -flushPeriod int
    	period in ms between flushes. metricPeriod must be cleanly divisible by flushPeriod. does not affect volume/throughput per se. the message is adjusted as to keep the volume/throughput constant (default 100)
  -keys-per-org int
    	how many metrics per orgs to simulate (default 100)
  -log-level int
    	log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL (default 2)
  -metricPeriod int
    	period in seconds between metric points (default 1)
  -nsqd-tcp-address string
    	nsqd TCP address (default "localhost:4150")
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
  -topic string
    	NSQ topic (default "metrics")
  -version
    	print version string

```

# there's 2 main use cases to run this tool:

mimic a real-time load of 40k metrics/s:

```
./fake_metrics_to_nsq -keys-per-org 100 -orgs 400 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150
```

or perform a backfill of data, in this example a years worth of data at a 40k x speedup, which takes about 13min.
obviously you need to use fewer metrics to do this.

```
./fake_metrics_to_nsq -keys-per-org 1 -orgs 1 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150 -offset 1y -speedup 40000
```


or a workload where you want the last days' worth of data to be filled but you also want a good amount of total metrics,
this gives a realistic workload for testing GC and such:

```
./fake_metrics_to_nsq -keys-per-org 100 -orgs 10 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150 -offset 1d -speedup 100 -stop-at-now
./fake_metrics_to_nsq -keys-per-org 100 -orgs 10 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150
```

