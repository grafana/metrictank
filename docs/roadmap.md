# Roadmap

## Tagging & metrics2.0

While Metrictank takes in tag metadata in the form of [metrics2.0](http://metrics20.org/) and indexes it, it is not exposed yet for querying.
There will be various benefits in adopting metrics2.0 fully (better choices for consolidation, data conversion, supplying unit information to Grafana, etc)
see [Tags](https://github.com/raintank/metrictank/blob/master/docs/tags.md)

## More advanced clustering

Current limitations: manual promotions, statically defined peers, per-instance primary status instead of per-shard, so you can't shuffle shard replicas across nodes.
see [clustering](https://github.com/raintank/metrictank/blob/master/docs/clustering.md) for more info)
