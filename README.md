[![Circle CI](https://circleci.com/gh/raintank/metrictank.svg?style=shield)](https://circleci.com/gh/raintank/metrictank)
[![Go Report Card](https://goreportcard.com/badge/github.com/raintank/metrictank)](https://goreportcard.com/report/github.com/raintank/metrictank)
[![GoDoc](https://godoc.org/github.com/raintank/metrictank?status.svg)](https://godoc.org/github.com/raintank/metrictank)


## introduction
*Metrictank is a multi-tenant, gorilla-inspired, cassandra-backed timeries database*

metrictank is a timeseries database, inspired by the [Facebook gorilla paper](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
Most notably, it employs the float64 compression mechanism described in the paper to dramatically lower storage overhead,
as well as data chunking to lower the load on cassandra.

## status

While [raintank](http://raintank.io) has been running metrictank in production since december 2015, there are still plenty of kinks to work out
and bugs to fix.  It should be considered an *alpha* project.

## limitations

* no performance/availability isolation between tenants per instance. (only data isolation)
* no sharding/partitioning mechanism in metrictank itself yet.  (Cassandra does it for storage)
* runtime master promotions (for clusters) are a manual process.
* no computation locality: we move the data from storage to processing code.  And we have 2 processing steps (metrictank and graphite-api)
* the datastructures can use performance engineering.   [Our workload exposes a shortcoming in the Go GC](https://github.com/golang/go/issues/14812) which may occasionally inflate response times.
* the native input protocol is currently not efficient.  Should split up data and metadata streams instead of sending all metadata with each point.
* we use metrics2.0 in native input protocol and indexes, but barely do anything with it yet.
* For any series you can't write points that are earlier than previously written points. (unless you restart MT)

## interesting design characteristics (feature or limitation.. up to you)

* only float64 values Ints and bools are stored as floats (works quite well due to the gorilla compression),
  No text support.  Might be revised later.
* only uint32 unix timestamps in second resolution.   For higher resolution, consider [streaming directly to grafana](https://blog.raintank.io/using-grafana-with-intels-snap-for-ad-hoc-metric-exploration/)
* no data locality: doesn't seem needed yet to put related series together.

## main features

* 100% open source
* graphite is a first class citizen.  You can use the [graphite-metrictank](https://github.com/raintank/graphite-metrictank) plugin, although
at this point it does require a [fork of graphite-api](https://github.com/raintank/graphite-api/) to run.
* Accurate, flexible rollups by storing min/max/sum/count (from which it can also compute the average at runtime).
So we can do consolidation (combined runtime+archived) accurately and correctly,
[unlike most other graphite backends like whisper](https://blog.raintank.io/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)
* metrictank acts as a writeback RAM cache for recent data.
* flexible tenancy: can be used single tenant, multi tenant (multile users who can't see each other's data), and selected data can be shared across all tenants.
* input options: carbon, metrics2.0, kafka. (soon: json or msgpack over http)
* guards against excessive data requests

## roadmap

#### tagging & metrics2.0

While Metrictank takes in tag metadata in the form of [metrics2.0](http://metrics20.org/) and indexes it, it is not exposed yet for querying.
There will be various benefits in adopting metrics2.0 fully (better choices for consolidation, data conversion, supplying unit information to Grafana, etc)
see tags.md

#### sharding / partitioning

As mentioned above Cassandra already does that for the storage layer, but at a certain point we'll need it for the memory layer as well.

## Docs

### installation, configuration and operation.

* [Quick start using docker](https://github.com/raintank/metrictank/blob/master/docs/quick-start-docker.md)
* [Installation guide](https://github.com/raintank/metrictank/blob/master/docs/installation.md)
* [Configuration](https://github.com/raintank/metrictank/blob/master/docs/config.md)
* [Data knobs](https://github.com/raintank/metrictank/blob/master/docs/data-knobs.md)
* [Cassandra](https://github.com/raintank/metrictank/blob/master/docs/cassandra.md)
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


License
=======

This software is copyright 2015 by Raintank, Inc. and is licensed under the
terms of the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

or in the root directory of this package.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
