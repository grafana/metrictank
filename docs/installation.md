# installation guide

## dependencies

* Cassandra. We run and recommend 3.0.8 .  We used to run 2.2.3 which was fine too. See cassandra.md
* Elasticsearch is currently a dependency for metrics metadata, but we will remove this soon.
* optionally a queue: Kafka 0.10 is recommended, but 0.9 should work too.
* currently you also need the [graphite-raintank finder plugin](https://github.com/raintank/graphite-metrictank)
  and our [graphite-api fork](https://github.com/raintank/graphite-api/).

## how things fit together

metrictank ingest metrics data. The data can be sent into it, or be read from a queue (see inputs.md)
metrictank will compress the data into chunks in RAM, a configurable amount of the most recent data
is kept in RAM, but the chunks are being saved to Cassandra as well.  You can use a single Cassandra
instance or a cluster.  Metrictank will also respond to queries: if the data is recent, it'll come out of
RAM, and older data is fetched from cassandra.  This happens transparantly.
Metrictank also needs elasticsearch to maintain an index of metrics metadata.
You'll typically query metrictank by querying graphite-api which uses the graphite-metrictank plugin to talk
to metrictank.  You can also query metrictank directly but this is very limited, experimental and not recommended.


## installation

### from source

Building metrictank requires a [Golang](https://golang.org/) compiler.
We recommend version 1.5 or higher.

```
go get github.com/raintank/metrictank
```

This installs only metrictank itself, and none of its dependencies.

### distribution packages

We automatically build rpms and debs on circleCi whenever the build succeeds.
These packages are pushed to packagecloud.
Releases are simply tagged versions like `0.5.1` ([releases](https://github.com/raintank/metrictank/releases)),
whereas commits in master following a release will be named `version-commit-after` for example `0.5.1-20` for
the 20th commit after `0.5.1`

We aim to keep master stable so that's your best bet.

[Get the metrictank packages here](https://packagecloud.io/app/raintank/raintank/search?filter=all&q=metrictank&dist=)

We also automatically publish packages for our other pieces you need:

* [our graphite-api fork](https://packagecloud.io/app/raintank/raintank/search?filter=all&q=graphite-api&dist=)
* [the graphite-metrictank finder plugin](https://packagecloud.io/app/raintank/raintank/search?filter=all&q=graphite-metrictank&dist=)

### chef cookbook

[chef_metric_tank](https://github.com/raintank/chef_metric_tank)

This installs only metrictank itself, and none of its dependencies.

## set up cassandra

You can have metrictank initialize Cassandra with a schema without replication, good for development setups.
Or you may want to tweak the schema yourself. See schema.md

## configuration

See the [example config file](https://github.com/raintank/metrictank/blob/master/metrictank-sample.ini) which guides you through the various options
