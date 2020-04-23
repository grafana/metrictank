# Metrictank

[![Circle CI](https://circleci.com/gh/grafana/metrictank.svg?style=shield)](https://circleci.com/gh/grafana/metrictank)
[![Go Report Card](https://goreportcard.com/badge/github.com/grafana/metrictank)](https://goreportcard.com/report/github.com/grafana/metrictank)
[![GoDoc](https://godoc.org/github.com/grafana/metrictank?status.svg)](https://godoc.org/github.com/grafana/metrictank)

## Introduction

Metrictank is a multi-tenant timeseries platform that can be used as a backend or replacement for Graphite.
It provides long term storage, high availability, efficient storage, retrieval and processing for large scale environments.

[GrafanaLabs](http://grafana.com) has been running metrictank in production since December 2015.
It currently requires an external datastore like Cassandra or Bigtable, and we highly recommend using Kafka to support clustering, as well
as a clustering manager like Kubernetes. This makes it non-trivial to operate, though GrafanaLabs has an on-premise product
that makes this process much easier.

## Features

* 100% open source
* Heavily compressed chunks (inspired by the [Facebook gorilla paper](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)) dramatically lower cpu, memory, and storage requirements and get much greater performance out of Cassandra than other solutions.
* Writeback RAM buffers and chunk caches, serving most data out of memory.
* Multiple rollup functions can be configured per serie (or group of series). E.g. min/max/sum/count/average, which can be selected at query time via consolidateBy().
So we can do consolidation (combined runtime+archived) accurately and correctly,
[unlike most other graphite backends like whisper](https://grafana.com/blog/2016/03/03/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)
* Flexible tenancy: can be used as single tenant or multi tenant. Selected data can be shared across all tenants.
* Input options: carbon, metrics2.0, kafka.
* Guards against excessively large queries. (per-request series/points restrictions)
* Data backfill/import from whisper
* Speculative Execution means you can use replicas not only for High Availability but also to reduce query latency.
* Write-Ahead buffer based on Kafka facilitates robust clustering and enables other analytics use cases.
* Tags and Meta Tags support
* Render response metadata: performance statistics, series lineage information and rollup indicator visible through Grafana
* Index pruning (hide inactive/stale series)
* Timeseries can change resolution (interval) over time, they will be merged seamlessly at read time. No need for any data migrations.

## Relation to Graphite

The goal of Metrictank is to provide a more scalable, secure, resource efficient and performant version of Graphite that is backwards compatible, while also adding some novel functionality.
(see Features, above)

There's 2 main ways to deploy Metrictank:
* as a backend for Graphite-web, by setting the `CLUSTER_SERVER` configuration value.
* as an alternative to a Graphite stack. This enables most of the additional functionality.  Note that Metrictank's API is not quite on par yet with Graphite-web:  some less commonly used functions are not implemented natively yet, in which case Metrictank relies on a graphite-web process to handle those requests. See [our graphite comparison page](docs/graphite.md) for more details.

## Limitations

* No performance/availability isolation between tenants per instance. (only data isolation)
* Minimum computation locality: we move the data from storage to processing code, which is both metrictank and graphite.
* Can't overwrite old data. We support reordering the most recent time window but that's it. (unless you restart MT)

## Interesting design characteristics (feature or limitation... up to you)

* Upgrades / process restarts requires running multiple instances (potentially only for the duration of the maintenance) and possibly re-assigning the primary role.
Otherwise data loss of current chunks will be incurred.  See [operations guide](https://github.com/grafana/metrictank/blob/master/docs/operations.md)
* clustering works best with an orchestrator like kubernetes. MT itself does not automate master promotions. See [clustering](https://github.com/grafana/metrictank/blob/master/docs/clustering.md) for more.
* Only float64 values. Ints and bools currently stored as floats (works quite well due to the gorilla compression),
* Only uint32 unix timestamps in second resolution.   For higher resolution, consider [streaming directly to grafana](https://grafana.com/blog/2016/03/31/using-grafana-with-intels-snap-for-ad-hoc-metric-exploration/)
* We distribute data by hashing keys, like many similar systems. This means no data locality (data that will be often used together may not live together)

## Docs

### installation, configuration and operation.

* [Overview](https://github.com/grafana/metrictank/blob/master/docs/overview.md)
* [Quick start using docker](https://github.com/grafana/metrictank/blob/master/docs/quick-start-docker.md)
  ([more docker information](docs/docker.md))
* [Installation guides](https://github.com/grafana/metrictank/blob/master/docs/installation.md)
* [Configuration](https://github.com/grafana/metrictank/blob/master/docs/config.md)
* [Memory server](https://github.com/grafana/metrictank/blob/master/docs/memory-server.md)
* [Compression tips](https://github.com/grafana/metrictank/blob/master/docs/compression-tips.md)
* [Cassandra](https://github.com/grafana/metrictank/blob/master/docs/cassandra.md)
* [Kafka](https://github.com/grafana/metrictank/blob/master/docs/kafka.md)
* [Inputs](https://github.com/grafana/metrictank/blob/master/docs/inputs.md)
* [Metrics](https://github.com/grafana/metrictank/blob/master/docs/metrics.md)
* [Operations](https://github.com/grafana/metrictank/blob/master/docs/operations.md)
* [Startup](https://github.com/grafana/metrictank/blob/master/docs/startup.md)
* [Tools](https://github.com/grafana/metrictank/blob/master/docs/tools.md)

### features in-depth

* [Clustering](https://github.com/grafana/metrictank/blob/master/docs/clustering.md)
* [Consolidation](https://github.com/grafana/metrictank/blob/master/docs/consolidation.md)
* [Multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md)
* [HTTP api](https://github.com/grafana/metrictank/blob/master/docs/http-api.md)
* [Graphite](https://github.com/grafana/metrictank/blob/master/docs/graphite.md)
* [Metadata](https://github.com/grafana/metrictank/blob/master/docs/metadata.md)
* [Tags](https://github.com/grafana/metrictank/blob/master/docs/tags.md)
* [Data importing](https://github.com/grafana/metrictank/blob/master/docs/data-importing.md)

### Other

* [Governance](https://github.com/grafana/metrictank/blob/master/GOVERNANCE.md)
* [Development and contributing](https://github.com/grafana/metrictank/blob/master/docs/CONTRIBUTING.md)
* [Community](https://github.com/grafana/metrictank/blob/master/docs/community.md)
* [Roadmap](https://github.com/grafana/metrictank/issues/1319)
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

Copyright 2016-2019 Grafana Labs

This software is distributed under the terms of the GNU Affero General Public License.

Some specific packages have a different license:
* schema : [apache2](schema/LICENSE)
* expr: [2-clause BSD](expr/LICENSE)
* mdata/chunk/tsz: [2-clause BSD](mdata/chunk/tsz/LICENSE)
