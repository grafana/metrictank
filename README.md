# Metrictank

[![Circle CI](https://circleci.com/gh/grafana/metrictank.svg?style=shield)](https://circleci.com/gh/grafana/metrictank)
[![Go Report Card](https://goreportcard.com/badge/github.com/grafana/metrictank)](https://goreportcard.com/report/github.com/grafana/metrictank)
[![GoDoc](https://godoc.org/github.com/grafana/metrictank?status.svg)](https://godoc.org/github.com/grafana/metrictank)

## Introduction

Metrictank is a multi-tenant timeseries engine for Graphite and friends.
It provides long term storage, high availability, efficient storage, retrieval and processing for large scale environments.

[GrafanaLabs](http://grafana.com) has been running metrictank in production since December 2015.
It currently requires an external datastore like Cassandra, and we highly recommend using Kafka to support clustering, as well
as a clustering manager like Kubernetes. This makes it non-trivial to operate, though GrafanaLabs has an on-premise product
that makes this process much easier.

## Features

* 100% open source
* Inspired by the [Facebook gorilla paper](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
Most notably, the heavily compressed chunks dramatically lower cpu, memory and storage requirements.
* Writeback RAM cache, serving most data out of memory.
* Graphite is a first class citizen. As of graphite-1.0.1, metrictank can be used as a graphite CLUSTER_SERVER.
* Can also act as a Graphite server itself, though the functions processing library is only partially implemented, metrictank proxies requests to Graphite if it can't handle the required processing (for those requests it will degrade to just being the backend storage)
* Can also be used with Prometheus (but the experience won't be as good as something built just for prometheus, which we're also working on)
* Accurate, flexible rollups by storing min/max/sum/count (which also gives us average).
So we can do consolidation (combined runtime+archived) accurately and correctly,
[unlike most other graphite backends like whisper](https://grafana.com/blog/2016/03/03/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)
* Flexible tenancy: can be used as single tenant or multi tenant. Selected data can be shared across all tenants.
* Input options: carbon, metrics2.0, kafka, Prometheus (soon: json or msgpack over http)
* Guards against excessive data requests

## Limitations

* No performance/availability isolation between tenants per instance. (only data isolation)
* Minimum computation locality: we move the data from storage to processing code, which is both metrictank and graphite.
* Backlog replaying and queries can be made faster. [A Go GC issue may occasionally inflate response times](https://github.com/golang/go/issues/14812).
* We use metrics2.0 in native input protocol and indexes, but [barely do anything with it yet](https://github.com/grafana/metrictank/blob/master/docs/tags.md).
* can't overwrite old data. We support reordering the most recent time window but that's it. (unless you restart MT)

## Interesting design characteristics (feature or limitation... up to you)

* Upgrades / process restarts requires running multiple instances (potentially only for the duration of the maintenance) and possibly re-assigning the primary role.
Otherwise data loss of current chunks will be incurred.  See [operations guide](https://github.com/grafana/metrictank/blob/master/docs/operations.md)
* clustering works best with an orchestrator like kubernetes. MT itself does not automate master promotions. See [clustering](https://github.com/grafana/metrictank/blob/master/docs/clustering.md) for more.
* Only float64 values. Ints and bools currently stored as floats (works quite well due to the gorilla compression),
  No text support.
* Only uint32 unix timestamps in second resolution.   For higher resolution, consider [streaming directly to grafana](https://grafana.com/blog/2016/03/31/using-grafana-with-intels-snap-for-ad-hoc-metric-exploration/)
* No data locality: doesn't seem needed yet to put related series together.


## Docs

### installation, configuration and operation.

* [Overview](https://github.com/grafana/metrictank/blob/master/docs/overview.md)
* [Quick start using docker](https://github.com/grafana/metrictank/blob/master/docs/quick-start-docker.md)
  ([other docker stacks](docs/docker.md))
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

* [Development and contributing](https://github.com/grafana/metrictank/blob/master/docs/CONTRIBUTING.md)
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
