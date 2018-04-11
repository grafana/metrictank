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

The Kafka input supports 2 formats:
* MetricData Messagepack-encoded (legacy: slow and verbose. they contain the data points as well as all metric data. see #876)
* MetricPoint messages. (more optimized: contains only id, value and timestamp. see #876)

See the [schema repository](https://github.com/raintank/schema) for more details.

This is the recommended input option if you want a queue. It also simplifies the operational model: since you can make nodes replay data
you don't have to reassign primary/secondary roles at runtime, you can just restart write nodes and have them replay data, for example.
Note that [carbon-relay-ng](https://github.com/graphite-ng/carbon-relay-ng) can be used to pipe a carbon stream into Kafka.

In the future we plan to do more optimisations such as:
* batch encoding instead of a kafka message per point.
* further compression (e.g. multiple points with shared timestamp).
