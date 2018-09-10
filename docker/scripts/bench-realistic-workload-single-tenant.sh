#!/bin/bash
trap 'kill $(jobs -p)' EXIT
# load up some old data
fakemetrics backfill --kafka-mdm-addr localhost:9092 --offset 5h --period 10s --speedup 100 --mpo 5000
# then continue with a realtime feed
fakemetrics feed --kafka-mdm-addr localhost:9092 --period 10s --mpo 5000 &
sleep 30
mt-index-cat -addr http://localhost:6063 -from 60min cass -hosts localhost:9042 -schema-file ./scripts/config/schema-idx-cassandra.toml vegeta-render-patterns | vegeta attack -duration 300s | vegeta report
sleep 20
