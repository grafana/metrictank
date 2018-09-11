#!/bin/bash
trap 'kill $(jobs -p)' EXIT

# this script aims to put a realistic workload on a metrictank instance or cluster
# (in terms of proportions, not absolute volume)
# the goal is not to test the limits of MT here or get close to production workloads,
# but rather use a workload that it can definitely handle
# so that we can analyze and optimize CPU/memory usage, GC latencies, etc.
# works best with docker-cluster, due to port 6063

# first load up some old data, at a high speedup rate, so we don't have to wait too long ( 3 minutes)
# lower speedup if your computer is too slow.  this is just to populate some old data, and is not what we care about here
fakemetrics backfill --kafka-mdm-addr localhost:9092 --offset 5h --period 10s --speedup 100 --mpo 10000
# then continue with a realtime feed. note: new series are being introduced here, because we want a sufficiently high ingest rate and num active series.
# but we couldn't possibly backfill this many series for a large timeframe in a short period
# this is realistic anyway. to have more recent data than old (eg. due to series churn and pruning)
fakemetrics feed --kafka-mdm-addr localhost:9092 --period 10s --mpo 100000 &
sleep 30
# now request series:
# some passthrough (requesting 1 series), some 1wildcard (upto 10 series), others 2wildcards (upto 100 series), others 3wildcards (upto 1000 series)
./build/mt-index-cat -addr http://localhost:6063 -from 60min cass -hosts localhost:9042 -schema-file scripts/config/schema-idx-cassandra.toml 'GET http://localhost:6063/render?target={{.Name | patternCustom 20 "pass" 30 "3rccw" 30 "2rccw" 20 "1rccw" }}&from=-1h\nX-Org-Id: 1\n\n' | vegeta attack -rate 20 -duration 300s | vegeta report
sleep 20
