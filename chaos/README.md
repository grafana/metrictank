# chaos

these golang tests spin up the docker-chaos clustered stack 
and perform various chaotic operations and confirm correct cluster behavior.

## dependencies

before running, download these containers: gaiadocker/iproute2, gaiaadm/pumba (actually for now checkout https://github.com/gaia-adm/pumba/pull/58 and build that locally)
(the go tests will automatically download them but it would mess up the timing results)

## how it works

12 kafka partitions. 12 metrics. one metric per partition.
6 MT instances in 3 pairs. (one primary and one secondary). each pair consumes the same 4 partitions (replication factor of 2)
So each instance "owns" (runs the primary shard containing) 4 metrics, and ingests data at 4 metrics per second, total workload across cluster is 24Hz.

## status

* working: testing of cluster startup, basic workload, and the effect of temporarily isolating 1 node min-available-shards 12 (all of them)
* todo: various other scenarios: isolate multiple shards, isolate multiple instances of the same shard, different min-available-shards-settings

