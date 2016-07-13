All input options - except for the carbon input - use the [metrics 2.0](http://metrics20.org/) format.
See the [schema repository](https://github.com/raintank/schema) for more details.

## general guidelines

compression works best if you don't have decimals. So if you have latencies in ms, best store them as 35ms for example, not as 0.035 seconds.

## carbon
useful for traditional graphite plaintext protocol.

http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-schemas-conf
It only uses this file to determine the raw interval of the metrics, and will ignore all retention durations
as well as intervals except for the first, highest-resolution (raw) one.


note: it does not implement [carbon2.0](http://metrics20.org/implementations/)



## kafka-mdm (recommended)

## kafka-mdam (experimental, discouraged)

This is a kafka input that uses application-level batches stored within single kafka messages.
It is discouraged because this does not allow proper routing/partitioning of messages and will be removed.
It only exists to compare performance numbers against kafka-mdm, and make mdm as fast as mdam.


## nsq (deprecated)
will be removed. NSQ does not guarantee ordering. Metric-tank needs ordered input for aggregations to work correctly,
and also for the compression to work optimally. The NSQ input "mostly works": we used to use it, but any out of order points
will flat out be dropped on the floor.
