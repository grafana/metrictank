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

This is the recommended input option if you want a queue. It also simplifies the operational model: since you can make nodes replay data
you don't have to reassign primary/secondary roles at runtime, you can just restart write nodes and have them replay data, for example.
Note that [carbon-relay-ng](https://github.com/graphite-ng/carbon-relay-ng) can be used to pipe a carbon stream into Kafka.

The Kafka input supports 2 formats:

* MetricData
* MetricPoint

Both formats have a corresponding implementation in the [schema repository](https://github.com/raintank/schema), making it trivial
to implement your own producers (and consumers) if you use Golang.

### MetricData

This format contains the data points as well as all metric identity data and metadata, in messagepack encoded messages (not JSON).
This format is rather verbose and inefficient to encode/decode.
See the [MetricData](https://godoc.org/github.com/raintank/schema#MetricData) documentation for format and implementation details.

### MetricPoint

This format is a hand-written binary format that is much more compact and fast to encode/decode compared to MetricData.
See the [MetricPoint](https://godoc.org/github.com/raintank/schema#MetricPoint) documentation for format and implementation details.
It is a minimal format that only contains the series identifier, value and timestamp.
As such, it is paramount that MetricPoint messages for each series have been preceded by a MetricData message for that series, so
that metrictank has had the chance to add the metric information into its index.
Otherwise metrictank will not recognize the ID and discard the point.

Note that the implementation has encode/decode function for the standard MetricPoint format, as well as a variant that does not encode the org-id
part of the series id.  For single-tenant environments, you can configure your producers and metrictank to not encode an org-id in all messages
and rather just set it in configuration, this makes the message more compact, but won't work in multi-tenant environments.

### Future formats

In the future we plan to do more optimisations such as:
* batch encoding instead of a kafka message per point.
* further compression (e.g. multiple points with shared timestamp).
