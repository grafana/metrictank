# Inputs

All input options - except for the carbon input - use the [metrics 2.0](http://metrics20.org/) format.
See the [schema repository](https://github.com/raintank/schema) for more details.


## How to send data to MT

see fakemetrics, tsdb-gw, carbon


## Carbon
useful for traditional graphite plaintext protocol.

http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-schemas-conf
It only uses this file to determine the raw interval of the metrics, and will ignore all retention durations
as well as intervals except for the first, highest-resolution (raw) one.


note: it does not implement [carbon2.0](http://metrics20.org/implementations/)



## Kafka-mdm (recommended)

`mdm = MetricData Messagepack-encoded` [MetricData schema definition](https://github.com/raintank/schema/blob/master/metric.go#L20)  
This is a kafka input wherein each point is sent as a unique kafka message. This is the best way,
even though we haven't gotten it to perform on par with kafka-mdam yet, but we expect to get there soon.
This is the recommended input option if you want a queue.

## Kafka-mdam (experimental, discouraged)

`mdm = MetricDataArray Messagepack-encoded` [MetricDatayArray schema definition](https://github.com/raintank/schema/blob/master/metric.go#L47)  
This is a kafka input that uses application-level batches stored within single kafka messages.
It is discouraged because this does not allow proper routing/partitioning of messages and will be removed.
It only exists to compare performance numbers against kafka-mdm, and make mdm as fast as mdam.


## NSQ (deprecated)
will be removed. NSQ does not guarantee ordering. Metrictank needs ordered input for aggregations to work correctly,
and also for the compression to work optimally. The NSQ input "mostly works": we used to use it, but any out of order points
will flat out be dropped on the floor.
