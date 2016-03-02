
# there's 2 main use cases to run this tool:

mimic a real-time high volume (lots of metrics) load:

```
./fake_metrics_to_nsq -keys-per-org 100 -orgs 200 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150 -offset 0 -steps 1 -interval 1
```

or perform a backfill of data, in this example a years worth of data at a 10k x speedup, which takes just under an hour.
obviously you need to use fewer metrics to do this.
```
./fake_metrics_to_nsq -keys-per-org 1 -orgs 1 -statsd-addr statsdaemon:8125 -nsqd-tcp-address nsqd:4150 -offset 31536000 -steps 10000 -interval 1

```
