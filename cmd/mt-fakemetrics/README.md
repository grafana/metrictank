# Fakemetrics

Fakemetrics generates a metrics workload with fake data, that you can feed into kafka, carbon, etc.

## Example invocations

Generate a real-time load of 40k metrics/s to kafka, metrics 1 second apart and flushed every second

```
mt-fakemetrics feed --kafka-mdm-addr localhost:9092
```

Generate a real-time load of 40k metrics/s to kafka, metrics 1 second apart and flushed every second. Metrics all have tags.

```
mt-fakemetrics feed --kafka-mdm-addr localhost:9092 --add-tags=true
```

Using unique values for 4 of the tags

```
mt-fakemetrics feed --kafka-mdm-addr localhost:9092 --add-tags=true --num-unique-tags=4
```

Using custom tags

```
mt-fakemtrics feed --kafka-mdm-addr localhost:9092 --custom-tags="key1=value1,key2=value2,key3=value3"
```

Using unique values for 1 of the custom tags

```
mt-fakemetrics feed --kafka-mdm-addr localhost:9092 --custom-tags="key1=value1,key2=value2,key3=value3" --num-unique-custom-tags=1
```

Generate a stream of historical data to kafka for 4 different organisations, 100 metrics each
with measurements 10 seconds apart for each metric, starting 5 hours ago and stopping at "now".
The speed is 100x what it would be if it were realtime (so a rate of 4x100x100=40kHz)

```
mt-fakemetrics backfill --kafka-mdm-addr localhost:9092 --offset 5h --period 10s --speedup 100 --orgs 4 --mpo 100
```

## Value Policies

If no Value Policy is specified a random float will be generated

You can specify a Value Policy with the `--value-policy` flag. Currently the following Value Policies are supported:

* single
* multiple
* timestamp

This flag can be useful when it is important to know what data fakemetrics generates to verify correctness of the data returned

### Single

This policy allows you to set one single value that all points will use. This makes it very easy to identify potential errors in your configuration or deployment.

Example:
```
mt-fakemetrics backfill --kafka-mdm-addr localhost:9092 --offset 5h --period 10s --speedup 100 --orgs 1 --mpo 100 --value-policy single:2.5
```

Note that there are no spaces in the policy setting itself.

### Multiple

The `multiple` policy allows you to pass in a list of values which will be iterated over. Upon reaching the end of your list it will start over at the beginning.

Example:
```
mt-fakemetrics backfill --kafka-mdm-addr localhost:9092 --offset 5h --period 10s --speedup 100 --orgs 1 --mpo 100 --value-policy multiple:1,2.152,3.14,4,5
```

Note that there are no spaces in the policy setting itself, and it uses commas to separate each value.

### Timestamp

This policy sets the value of each point to its timestamp.

```
mt-fakemetrics backfill --kafka-mdm-addr localhost:9092 --offset 5h --period 10s --speedup 100 --orgs 1 --mpo 100 --value-policy timestamp
```

Do not use a colon (':') with this policy, as you can't specify a value with this.

## Outputs

kafka and stdout are multi-tenant outputs where structured data is sent and multiple orgs may have the same key in their own namespace.
carbon and gnet (short for grafana.net or more specifically the [tsdb-gw](https://github.com/raintank/tsdb-gw) service) are single-tenant.
so in that case you can only simulate one org otherwise the keys would overwrite each other.
for the gnet output, the org-id will be set to whatever you authenticate as (unless you use the admin key),

## Important

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
