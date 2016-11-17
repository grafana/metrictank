# Inputs

All input options - except for the carbon input - use the [metrics 2.0](http://metrics20.org/) format.
See the [schema repository](https://github.com/raintank/schema) for more details.


## How to send data to MT

see fakemetrics, tsdb-gw, carbon


## Carbon
useful for traditional graphite plaintext protocol.  Does not support pickle format.

** Important: this input requires a
[carbon storage-schemas.conf](http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-schemas-conf) file.
Metrictank uses this file to determine the raw interval of the metrics, but it ignores all retention durations
as well as intervals after the first, raw one since metrictank already has its own config mechanism
for retention and aggregation. **

note: it does not implement [carbon2.0](http://metrics20.org/implementations/)


## Kafka-mdm (recommended)

`mdm = MetricData Messagepack-encoded` [MetricData schema definition](https://github.com/raintank/schema/blob/master/metric.go#L20)  
This is a kafka input wherein each point is sent as a unique kafka message.
This is the recommended input option if you want a queue.


