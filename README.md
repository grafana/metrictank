[![Circle CI](https://circleci.com/gh/grafana/metrictank.svg?style=shield)](https://circleci.com/gh/grafana/metrictank)
[![Go Report Card](https://goreportcard.com/badge/github.com/grafana/metrictank)](https://goreportcard.com/report/github.com/grafana/metrictank)
[![GoDoc](https://godoc.org/github.com/grafana/metrictank?status.svg)](https://godoc.org/github.com/grafana/metrictank)


## introduction
*Metrictank is a cassandra-backed, metrics2.0 based, multi-tenant timeseries database for Graphite and friends*

metrictank is a timeseries database, inspired by the [Facebook gorilla paper](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
Most notably, it employs the compression mechanism described in the paper to dramatically lower storage overhead,
as well as data chunking to lower the load on cassandra.  Graphite users are first-class citizens.

## status

While [GrafanaLabs](http://grafana.com) has been running metrictank in production since december 2015, there are still plenty of kinks to work out
and bugs to fix.  It should be considered an *alpha* project.

## limitations

* no performance/availability isolation between tenants per instance. (only data isolation)
* clustering is basic: statically defined peers, master promotions are manual, etc. See [clustering](https://github.com/grafana/metrictank/blob/master/docs/clustering.md) for more.
* minimum computation locality: we move the data from storage to processing code, which is both metrictank and graphite.
* the datastructures can use performance engineering.   [A Go GC issue may occasionally inflate response times](https://github.com/golang/go/issues/14812).
* the native input protocol is inefficient.  Should not send all metadata with each point.
* we use metrics2.0 in native input protocol and indexes, but [barely do anything with it yet](https://github.com/grafana/metrictank/blob/master/docs/tags.md).
* for any series you can't write points that are earlier than previously written points. (unless you restart MT)

## interesting design characteristics (feature or limitation.. up to you)

* upgrades / process restarts requires running multiple instances (potentially only for the duration of the maintenance) and re-assigning the primary role.
Otherwise data loss of current chunks will be incurred.  See [operations guide](https://github.com/grafana/metrictank/blob/master/docs/operations.md)
* only float64 values. Ints and bools currently stored as floats (works quite well due to the gorilla compression),
  No text support.
* only uint32 unix timestamps in second resolution.   For higher resolution, consider [streaming directly to grafana](https://grafana.com/blog/2016/03/31/using-grafana-with-intels-snap-for-ad-hoc-metric-exploration/)
* no data locality: doesn't seem needed yet to put related series together.

## main features

* 100% open source
* graphite is a first class citizen. As of graphite-1.0.1, metrictank can be used as a graphite CLUSTER_SERVER.
* accurate, flexible rollups by storing min/max/sum/count (which also gives us average).
So we can do consolidation (combined runtime+archived) accurately and correctly,
[unlike most other graphite backends like whisper](https://grafana.com/blog/2016/03/03/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)
* metrictank acts as a writeback RAM cache for recent data.
* flexible tenancy: can be used as single tenant or multi tenant. Selected data can be shared across all tenants.
* input options: carbon, metrics2.0, kafka. (soon: json or msgpack over http)
* guards against excessive data requests
* efficient data compression and efficient use of Cassandra.

## Docs

### installation, configuration and operation.

* [Overview](https://github.com/grafana/metrictank/blob/master/docs/overview.md)
* [Quick start using docker](https://github.com/grafana/metrictank/blob/master/docs/quick-start-docker.md)
* [Installation guides](https://github.com/grafana/metrictank/blob/master/docs/installation.md)
* [Configuration](https://github.com/grafana/metrictank/blob/master/docs/config.md)
* [Memory server](https://github.com/grafana/metrictank/blob/master/docs/memory-server.md)
* [Compression tips](https://github.com/grafana/metrictank/blob/master/docs/compression-tips.md)
* [Cassandra](https://github.com/grafana/metrictank/blob/master/docs/cassandra.md)
* [Kafka](https://github.com/grafana/metrictank/blob/master/docs/kafka.md)
* [Inputs](https://github.com/grafana/metrictank/blob/master/docs/inputs.md)
* [Metrics](https://github.com/grafana/metrictank/blob/master/docs/metrics.md)
* [Operations](https://github.com/grafana/metrictank/blob/master/docs/operations.md)
* [Tools](https://github.com/grafana/metrictank/blob/master/docs/tools.md)

### features in-depth

* [Clustering](https://github.com/grafana/metrictank/blob/master/docs/clustering.md)
* [Consolidation](https://github.com/grafana/metrictank/blob/master/docs/consolidation.md)
* [Multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md)
* [HTTP api](https://github.com/grafana/metrictank/blob/master/docs/http-api.md)
* [Graphite](https://github.com/grafana/metrictank/blob/master/docs/graphite.md)
* [Metadata](https://github.com/grafana/metrictank/blob/master/docs/metadata.md)
* [Tags](https://github.com/grafana/metrictank/blob/master/docs/tags.md)

### Other

* [Development](https://github.com/grafana/metrictank/blob/master/docs/development.md)
* [Community](https://github.com/grafana/metrictank/blob/master/docs/community.md)
* [Roadmap](https://github.com/grafana/metrictank/blob/master/docs/roadmap.md)
* [Faq](https://github.com/grafana/metrictank/blob/master/docs/faq.md)

## Releases and versioning

* [releases and changelog](https://github.com/grafana/metrictank/releases)
* we aim to keep `master` stable and vet code before merging to master
* We're pre-1.0 but adopt semver for our `0.MAJOR.MINOR` format. The rules are simple:
  * MAJOR version for incompatible API or functionality changes
  * MINOR version when you add functionality in a backwards-compatible manner, and

  We don't do patch level releases since minor releases are frequent enough.


License
=======

Copyright 2016-2018 Dieter Plaetinck, Anthony Woods, Jeremy Bingham, Damian Gryski, raintank inc

This software is distributed under the terms of the GNU Affero General Public License.
