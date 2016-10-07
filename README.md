[![Circle CI](https://circleci.com/gh/raintank/metrictank.svg?style=shield)](https://circleci.com/gh/raintank/metrictank)
[![Go Report Card](https://goreportcard.com/badge/github.com/raintank/metrictank)](https://goreportcard.com/report/github.com/raintank/metrictank)
[![GoDoc](https://godoc.org/github.com/raintank/metrictank?status.svg)](https://godoc.org/github.com/raintank/metrictank)


## introduction
*Metrictank is a cassandra-backed, metrics2.0 based, multi-tenant timeseries database for Graphite and friends*

metrictank is a timeseries database, inspired by the [Facebook gorilla paper](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
Most notably, it employs the compression mechanism described in the paper to dramatically lower storage overhead,
as well as data chunking to lower the load on cassandra.  Graphite users are first-class citizens.

## status

While [raintank](http://raintank.io) has been running metrictank in production since december 2015, there are still plenty of kinks to work out
and bugs to fix.  It should be considered an *alpha* project.

## limitations

* no performance/availability isolation between tenants per instance. (only data isolation)
* no sharding/partitioning mechanism in metrictank itself yet.  (Cassandra does it for storage) ([work in progress](https://github.com/raintank/metrictank/issues/315))
* runtime master promotions (for clusters) are a manual process.
* no computation locality: we move the data from storage to processing code, which is both metrictank and graphite-api.
* the datastructures can use performance engineering.   [A Go GC issue may occassionally inflate response times](https://github.com/golang/go/issues/14812).
* the native input protocol is inefficient.  Should not send all metadata with each point.
* we use metrics2.0 in native input protocol and indexes, but [barely do anything with it yet](https://github.com/raintank/metrictank/blob/master/docs/tags.md).
* for any series you can't write points that are earlier than previously written points. (unless you restart MT)

## interesting design characteristics (feature or limitation.. up to you)

* upgrades / process restarts requires running multiple instances (potentially only for the duration of the maintenance) and re-assigning the primary role.
Otherwise data loss of current chunks will be incurred.  See [operations guide](https://github.com/raintank/metrictank/blob/master/docs/operations.md)
* only float64 values. Ints and bools currently stored as floats (works quite well due to the gorilla compression),
  No text support.
* only uint32 unix timestamps in second resolution.   For higher resolution, consider [streaming directly to grafana](https://blog.raintank.io/using-grafana-with-intels-snap-for-ad-hoc-metric-exploration/)
* no data locality: doesn't seem needed yet to put related series together.

## main features

* 100% open source
* graphite is a first class citizen (note: currently requires a [fork of graphite-api](https://github.com/raintank/graphite-api/)
  and the [graphite-metrictank](https://github.com/raintank/graphite-metrictank) plugin)
* accurate, flexible rollups by storing min/max/sum/count (which also gives us average).
So we can do consolidation (combined runtime+archived) accurately and correctly,
[unlike most other graphite backends like whisper](https://blog.raintank.io/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)
* metrictank acts as a writeback RAM cache for recent data.
* flexible tenancy: can be used as single tenant or multi tenant. Selected data can be shared across all tenants.
* input options: carbon, metrics2.0, kafka. (soon: json or msgpack over http)
* guards against excessive data requests
* efficient data compression and efficient use of Cassandra.

## Docs

### installation, configuration and operation.

* [Overview](https://github.com/raintank/metrictank/blob/master/docs/overview.md)
* [Quick start using docker](https://github.com/raintank/metrictank/blob/master/docs/quick-start-docker.md)
* [Installation guides](https://github.com/raintank/metrictank/blob/master/docs/installation.md)
* [Configuration](https://github.com/raintank/metrictank/blob/master/docs/config.md)
* [Data knobs](https://github.com/raintank/metrictank/blob/master/docs/data-knobs.md)
* [Cassandra](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md)
* [Kafka](https://github.com/raintank/metrictank/blob/master/docs/kafka.md)
* [Inputs](https://github.com/raintank/metrictank/blob/master/docs/inputs.md)
* [Metrics](https://github.com/raintank/metrictank/blob/master/docs/metrics.md)
* [Operations](https://github.com/raintank/metrictank/blob/master/docs/operations.md)

### features in-depth

* [Clustering](https://github.com/raintank/metrictank/blob/master/docs/clustering.md)
* [Consolidation](https://github.com/raintank/metrictank/blob/master/docs/consolidation.md)
* [HTTP api](https://github.com/raintank/metrictank/blob/master/docs/http-api.md)
* [Metadata](https://github.com/raintank/metrictank/blob/master/docs/metadata.md)
* [Tags](https://github.com/raintank/metrictank/blob/master/docs/tags.md)
* [Usage reporting](https://github.com/raintank/metrictank/blob/master/docs/usage-reporting.md)

### Other

* [Development](https://github.com/raintank/metrictank/blob/master/docs/development.md)
* [Community](https://github.com/raintank/metrictank/blob/master/docs/community.md)
* [Roadmap](https://github.com/raintank/metrictank/blob/master/docs/roadmap.md)


License
=======

Copyright 2016 Dieter Plaetinck, Anthony Woods, Jeremy Bingham, Damian Gryski, raintank inc

This software is distributed under the terms of the GNU Affero General Public License.
