# Roadmap

Note: this is the roadmap covering the entire "Graphite Platform" at Grafana Labs.
It includes metrictank, graphite, [tsdb-gw](https://github.com/raintank/tsdb-gw/), [carbon-relay-ng](https://github.com/graphite-ng/carbon-relay-ng/) and hosted-metrics-api.

It is indicative, incomplete and subject to change.

## Q2 2019

* Sharding by tags
* Whisper importer for bigtable
* Meta tags
* Formalize tag format. Shared validation library
* Management UI for private cloud / enterprise customers to admin their cluster
* Documentation:
  - platform functionality, api’s (ingestion, querying, metrics management) (cloud)
  - Administrator Guide (on-prem)
* Rollup indicator in Grafana -> return metadata with render responses
* Index RAM optimizations v1: string interning
* Dot munging overhaul - needed tech debt fixing for string interning
* Kafka TLS and Auth support
* refactor cluster migration
* carbon-relay-ng counter aggregator
* carbon-relay-ng fix aggregator unreliable under cpu load
* carbon-relay-ng test all packages / packaging bugs

# Q3 2019

* Agent v2 (with insights into config, support to push config updates, feedback to customers about perf issues, HA without out-of-order issues)
* stats alongside query responses
* Index reporting (break down of series counts by subpath)
* Rate limits on inbound rate + active series and queries
* Opentracing sampling, on-demand tracing of requests
* Slow query log
* RAM reductions (stream chunks instead of point slices, jit decoding, etc)
* Non-k8s installation
* Continuous deployment to GrafanaCloud
* Graphite render api 100% native

# Q4 2019

* Clustering/scalability refactor
* Tooling for bigtable on par with cassandra?

