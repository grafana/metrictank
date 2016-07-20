# installation guide

## dependencies

* Cassandra. TODO version ? Mostly tested with Cassandra 2.2.3
* Elasticsearch is currently a dependency for metrics metadata, but we will remove this soon.
* optionally a queue: Kafka 0.10 is reccomended, but 0.9 should work too.

## installation

### from source

Building metrictank requires a [Golang](https://golang.org/) compiler.
We recommend version 1.5 or higher.

```
go get github.com/raintank/metrictank
```

### distribution packages

#### bleeding edge packages

https://packagecloud.io/app/raintank/raintank/search?filter=all&q=metrictank&dist=

#### stable packages

TODO: stable packages, rpms

### using chef
https://github.com/raintank/chef_metric_tank

### docker

TODO

## set up cassandra

You can have metrictank initialize Cassandra with a schema without replication, good for development setups.
Or you may want to tweak the schema yourself. See schema.md

## configuration

See the [example config file](https://github.com/raintank/metrictank/blob/master/metrictank-sample.ini) which guides you through the various options
