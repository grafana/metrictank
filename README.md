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

* no strong isolation between tenants (other than to make sure they can't see each other's data).  
  So tenants could negatively impact the performance or availability for others.
* no sharding/partitioning mechanism built into metrictank itself yet.  
  Cassandra of course does this on the data storage level
* runtime master promotions (for clusters) are a manual process.
* no computation locality:   
  - we pull in (raw or pre-rolled-up) data first from cassandra
  - then process and possibly consolidate it further in metrictank
  - further processing/aggregation (averaging series together etc) can happen in Graphite.  
  At a certain scale we will need to move the computation to the data, but we don't have that problem yet,  
  though we do plan to move more of the graphite logic into metrictank and further develop graphite-ng.
* many of the key datastructures need to be redesigned for better performance and lower GC pressure.  
  (there's a whole lot of pointers and strings which even [exposes a shortcoming in the Go GC](https://github.com/golang/go/issues/14812)
  which can trigger elevated request times (in the seconds range) when a GC runs)
* the input protocol is currently unoptimized and inefficient.   
  For one thing we have to split up the data and metadata streams instead of sending all metadata with each point.
* currently impossible to write back in time. E.g. for any series you can't write points that are earlier than previously written points.

## interesting design characteristics (feature or limitation.. up to you)

* only deals with float64 values. No ints, bools, text, etc.  
  Some type optimisations may come, though using the float type for ints and bools works quite well thanks to the clever gorilla compression.
* only uint32 unix timestamps in second resolution.   
  We found higher-resolution is more useful for ad-hoc debugging, where you can
  [stream directly to grafana and bypass the database](https://blog.raintank.io/using-grafana-with-intels-snap-for-ad-hoc-metric-exploration/)
* no data locality: we don't have anything that puts related series together.   
  This may help with read performance but we haven't needed to look into this yet.


## main features


#### 100% open source

cause that's how we roll.


#### graphite integration

Graphite is a first class citizen for metrictank.  You can use the [graphite-metrictank](https://github.com/raintank/graphite-metrictank) plugin, although
at this point it does require a [fork of graphite-api](https://github.com/raintank/graphite-api/) to run.  We're working on compatibility with graphite-web.


#### better roll-ups

Metrictank can store rollups for all your series.  Each rollup is currently 4 series: min/max/sum/count (from which it can also compute the average at runtime).
This means we can do consolidation (by combining archived rollups with runtime consolidation) accurately and correctly,
[unlike most other graphite backends like whisper](https://blog.raintank.io/25-graphite-grafana-and-statsd-gotchas/#runtime.consolidation)

#### in-memory component for hot data

Metrictank is essentially a (clustered) write-back cache for cassandra, with configurable retention in RAM.  Most of your queries (for recent data) will come out of
RAM and will be quite fast.  Thanks to dropping RAM prices and the gorilla compression, you can hold a lot of data in RAM this way.

#### multi-tenancy

Metrictank supports multiple tenants (e.g. users) that each have their own isolated data within the system, and can't see other users' data.
Note:
* you can simply use it as a single-tenant system by only using 1 organisation
* you can also share data that every tenant can see by publish as org -1

#### ingestion options:

metrics2.0, kafka, carbon, json or msgpack over http.

#### guards against excessive data requests

## roadmap

#### tagging & metrics2.0

While Metrictank takes in tag metadata in the form of [metrics2.0](http://metrics20.org/) and indexes it, it is not exposed yet for querying.
There will be various benefits in adopting metrics2.0 fully (better choices for consolidation, data conversion, supplying unit information to Grafana, etc)
see tags.md

#### sharding / partitioning

As mentioned above Cassandra already does that for the storage layer, but at a certain point we'll need it for the memory layer as well.



## Help, more info, documentation, ...

[community slack](http://slack.raintank.io/). This is the raintank slack, it has a metrictank room (as well as a grafana room and a room for all of our products)

[docs](https://github.com/raintank/metrictank/tree/master/docs)

For help or questions, you can also just open tickets in GitHub.



License
=======

This software is copyright 2015 by Raintank, Inc. and is licensed under the
terms of the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

or in the root directory of this package.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
