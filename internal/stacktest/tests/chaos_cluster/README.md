# chaos

these golang tests spin up the docker-chaos clustered stack 
and perform various chaotic operations and confirm correct cluster behavior.

## dependencies

before running, download these containers: gaiadocker/iproute2, gaiaadm/pumba
(the go tests will automatically download them but it would mess up the timing results)

## how it works

### ingestion load

12 kafka partitions. 12 metrics. one metric per partition.
6 MT instances in 3 pairs. (one primary and one secondary). each pair consumes the same 4 partitions (replication factor of 2)
So each instance "owns" (runs the primary shard containing) 4 metrics, and ingests data at 4 metrics per second, total workload across cluster is 24Hz.

### tested scenarios

* TestClusterStartup: validates all components have come up
* TestClusterBaseIngestWorkload : makes sure that all metrictanks' ingestion stats report exactly 4 metrics received per second (per above)
* TestQueryWorkload: validate that all metrictanks respond correctly to a query that requires all data
* TestIsolateOneInstance: isolates metrictank-4 from the rest of the cluster for 30s, during a 60s test. validates that all requests against all other metrictanks work as normal the entire time, and all requests issued against metrictank-4 fail during the isolation, and become normal when the instance rejoins.

## future work

* various other scenarios: isolate multiple shards, isolate multiple instances of the same shard, different min-available-shards-settings

