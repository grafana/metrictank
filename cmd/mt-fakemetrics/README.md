## fakemetrics

fakemetrics generates a metrics workload with fake data, that you can feed into kafka, carbon, etc.

install like so:
```
go get -u github.com/raintank/fakemetrics
```

## invocation

```
fakemetrics -h
Generates fake metrics workload

Usage:
  fakemetrics [command]

Available Commands:
  agents           Mimic independent agents
  agginput         A particular workload good to test performance of carbon-relay-ng aggregators
  backfill         backfills old data and stops when 'now' is reached
  bad              Sends out invalid/out-of-order/duplicate metric data
  feed             Publishes a realtime feed of data
  help             Help about any command
  resolutionchange Sends out metric with changing intervals, time range 24hours
  storageconf      Sends out one or more set of 10 metrics which you can test aggregation and retention rules on
  version          Print the version number

Flags:
  -t, --add-tags                     add the built-in tags to generated metrics (default false)
      --carbon-addr string           carbon TCP address. e.g. localhost:2003
      --config string                config file (default is $HOME/.fakemetrics.yaml)
      --custom-tags strings          A list of comma separated tags (i.e. "tag1=value1,tag2=value2")(default empty) conflicts with add-tags
      --gnet-addr string             gnet address. e.g. http://localhost:8081
      --gnet-key string              gnet api key
  -h, --help                         help for fakemetrics
      --kafka-comp string            compression: none|gzip|snappy (default "snappy")
      --kafka-mdam-addr string       kafka TCP address for MetricDataArray-Msgp messages. e.g. localhost:9092
      --kafka-mdm-addr string        kafka TCP address for MetricData-Msgp messages. e.g. localhost:9092
      --kafka-mdm-topic string       kafka topic for MetricData-Msgp messages (default "mdm")
      --kafka-mdm-v2                 enable MetricPoint optimization (send MetricData first, then optimized MetricPoint payloads) (default true)
      --listen string                http listener address for pprof. (default ":6764")
      --log-level int                log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL (default 2)
      --num-unique-custom-tags int   a number between 0 and the length of custom-tags. when using custom-tags this will make the tags unique (default 0)
      --num-unique-tags int          a number between 0 and 10. when using add-tags this will add a unique number to some built-in tags (default 1)
      --partition-scheme string      method used for partitioning metrics (kafka-mdm-only). (byOrg|bySeries|bySeriesWithTags|bySeriesWithTagsFnv|lastNum) (default "bySeries")
      --statsd-addr string           statsd TCP address. e.g. 'localhost:8125'
      --statsd-type string           statsd type: standard or datadog (default "standard")
      --stdout                       enable emitting metrics to stdout

Use "fakemetrics [command] --help" for more information about a command.

```

## Example invocations

Generate a real-time load of 40k metrics/s to kafka, metrics 1 second apart and flushed every second

```
fakemetrics feed --kafka-mdm-addr localhost:9092
```

Generate a real-time load of 40k metrics/s to kafka, metrics 1 second apart and flushed every second. Metrics all have tags.

```
fakemetrics feed --kafka-mdm-addr localhost:9092 --add-tags=true
```

Using unique values for 4 of the tags

```
fakemetrics feed --kafka-mdm-addr localhost:9092 --add-tags=true --num-unique-tags=4
```

Using custom tags

```
fakemtrics feed --kafka-mdm-addr localhost:9092 --custom-tags="key1=value1,key2=value2,key3=value3"
```

Using unique values for 1 of the custom tags

```
fakemetrics feed --kafka-mdm-addr localhost:9092 --custom-tags="key1=value1,key2=value2,key3=value3" --num-unique-custom-tags=1
```

Generate a stream of historical data to kafka for 4 different organisations, 100 metrics each
with measurements 10 seconds apart for each metric, starting 5 hours ago and stopping at "now".
The speed is 100x what it would be if it were realtime (so a rate of 4x100x100=40kHz)

```
fakemetrics backfill --kafka-mdm-addr localhost:9092 --offset 5h --period 10s --speedup 100 --orgs 4 --mpo 100
```

# Outputs

kafka and stdout are multi-tenant outputs where structured data is sent and multiple orgs may have the same key in their own namespace.
carbon and gnet (short for grafana.net or more specifically the [tsdb-gw](https://github.com/raintank/tsdb-gw) service) are single-tenant.
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
