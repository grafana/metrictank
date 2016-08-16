# installation guide

## dependencies

* Cassandra. We run and recommend 3.0.8 . See
[Cassandra](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md)
* Elasticsearch is currently a dependency for metrics metadata, but we will remove this soon.
  See [metadata in ES](https://github.com/raintank/metrictank/blob/master/docs/metadata.md#es)
* optionally a queue if you want to buffer data in case metrictank goes down: Kafka 0.10 is recommended, but 0.9 should work too.
* currently you also need the [graphite-raintank finder plugin](https://github.com/raintank/graphite-metrictank)
  and our [graphite-api fork](https://github.com/raintank/graphite-api/). (which we install as 1 piece)

## how things fit together

metrictank ingest metrics data. The data can be sent into it, or be read from a queue (see
[Inputs](https://github.com/raintank/metrictank/blob/master/docs/inputs.md))
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

We automatically build rpms and debs on circleCi for all needed components whenever the build succeeds.
These packages are pushed to packagecloud.

[Instructions to enable the raintank packagecloud repository](https://packagecloud.io/raintank/raintank/install)

You need to install these packages:

* metrictank
* graphite-metrictank (includes both our graphite-api variant as well as the graphite-metrictank finder plugin)

Releases are simply tagged versions like `0.5.1` ([releases](https://github.com/raintank/metrictank/releases)),
whereas commits in master following a release will be named `version-commit-after` for example `0.5.1-20` for
the 20th commit after `0.5.1`

We aim to keep master stable so that's your best bet.

Supported distributions:

* Ubuntu 14.04, 16.04
* Debian Wheezy, Jessie
* Centos 6, 7

### chef cookbook

[chef_metric_tank](https://github.com/raintank/chef_metric_tank)

This installs only metrictank itself, and none of its dependencies.

## set up cassandra

For basic setups, you can just install it and start it with default settings.
To tweak schema and settings, see [Cassandra](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md)

## set up elasticsearch

Also here, you can just install it and start it with default settings. 

## optional: set up kafka

You can run a persistent queue in front of metrictank.
If your metric instance(s) go down, then a queue is helpful in buffering and saving all the data while your instance(s) is/are down.
The moment your metrictank instance(s) come(s) back up, they can replay everything they missed (and more, it's useful to load in older data
so that you can serve queries for it out of RAM).
Also, in case you want to make any change to your aggregations, Cassandra cluster, or whatever, it can be useful to re-process older data.

** Note: the above actually doesn't work yet, as we don't have the seek-back-in-time implemented yet to fetch old data from Kafka.
So for now using Kafka is more about preparing for the future than getting immediate benefit. **

You can install Kafka. Ideally 0.10 or later.
Then just run it.  Default settings are fine.

## configuration

See the [example config file](https://github.com/raintank/metrictank/blob/master/metrictank-sample.ini) which guides you through the various options
