[![Circle CI](https://circleci.com/gh/raintank/metrictank.svg?style=shield)](https://circleci.com/gh/raintank/metrictank)
[![Go Report Card](https://goreportcard.com/badge/github.com/raintank/metrictank)](https://goreportcard.com/report/github.com/raintank/metrictank)
[![GoDoc](https://godoc.org/github.com/raintank/metrictank?status.svg)](https://godoc.org/github.com/raintank/metrictank)

# metrictank

is a multi-tenant, gorilla-inspired, cassandra-backed timeries database 

## introduction

metrictank is a timeseries database, inspired by the [Facebook gorilla paper](www.vldb.org/pvldb/vol8/p1816-teller.pdf).
Most notably, it employs the float64 compression mechanism described in the paper to dramatically lower storage overhead,
as well as data chunking to lower the load on cassandra.

## status

While [raintank](raintank.io) has been running it in production since december 2015, there are still plenty of kinks to work out
and bugs to fix.  It should be considered an *alpha* project.

## limitations

* no strong isolation between tenants (other than to make sure they can't see each other's data). Tenants could negatively impact the performance for others.
* no sharding/partitioning mechanism built-in.
* master promotion is a manual process.
* no computation locality: we pull in all the raw data first from cassandra, then process/consolidate it in metric-tank. Further processing/aggregation happens in Graphite.  At a certain scale you need to move the computation to the data, but we don't have that problem yet, though we do plan to move more of the graphite logic into metric-tank and further develop graphite-ng.

## interesting design characteristics (feature or limitation.. up to you)

* only deals with float64 values. No ints, bools, text, etc. Some type optimisations may come, though using the float type for ints and bools works quite well thanks to the compression.
* only uint32 unix timestamps in second resolution. We found higher-resolution is more useful for ad-hoc debugging, where you can [stream directly to grafana and can bypass the database](https://blog.raintank.io/using-grafana-with-intels-snap-for-ad-hoc-metric-exploration/)
* no data locality: we don't have anything that puts related series together.  Often this helps with read performance but we haven't needed to look into this yet.


## main features


### 100% open source

cause that's how we roll.


### graphite integration

https://github.com/raintank/graphite-raintank


### roll-ups

### in-memory component for hot data

### multi-tenancy

### ingestion options:

metrics2.0, kafka, carbon, json or msgpack over http.

### guards against excessive data requests

## roadmap

### tagging & metrics2.0

While Metrictank takes in tag metadata in the form of [metrics2.0](http://metrics20.org/) and indexes it, it is not exposed yet for querying.
Adopting metrics2.0 fully will help with picking better defaults for consolidation.



License
=======

This software is copyright 2015 by Raintank, Inc. and is licensed under the
terms of the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

or in the root directory of this package.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
