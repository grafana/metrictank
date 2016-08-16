# Roadmap

## Tagging & metrics2.0

While Metrictank takes in tag metadata in the form of [metrics2.0](http://metrics20.org/) and indexes it, it is not exposed yet for querying.
There will be various benefits in adopting metrics2.0 fully (better choices for consolidation, data conversion, supplying unit information to Grafana, etc)
see [Tags](https://github.com/raintank/metrictank/blob/master/docs/tags.md)

## Sharding / partitioning

As mentioned above Cassandra already does that for the storage layer, but at a certain point we'll need it for the memory layer as well.

