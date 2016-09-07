# why

Kafka can be used as an [ingestion option](https://github.com/raintank/metrictank/blob/master/docs/inputs.md) as well as a [clustering transport](https://github.com/raintank/metrictank/blob/master/docs/clustering.md) for metrictank.

# Kafka version

We recommend 0.10.0.1 or higher.

If you use 0.10.0.0 and want snappy compression, watch out for [kafka-3789](https://issues.apache.org/jira/browse/KAFKA-3789) as you'll need to do a hack [like this](https://github.com/raintank/raintank-docker/commit/e98883b08f343d896a3333801f16c7a603e89422)

0.9 should work too (we used to use it), but we don't support it.
