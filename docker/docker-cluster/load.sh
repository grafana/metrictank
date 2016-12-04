#!/bin/bash
trap 'kill $(jobs -p)' EXIT
fakemetrics -kafka-mdm-tcp-address localhost:9092 -statsd-addr localhost:8125 -shard-org -keys-per-org 1000 -orgs 2 -flushPeriod 250 -speedup 20 -stop-at-now -offset 60min
fakemetrics -kafka-mdm-tcp-address localhost:9092 -statsd-addr localhost:8125 -shard-org -keys-per-org 1000 -orgs 2 -flushPeriod 250 &
inspect-idx -from=1h -addr http://metrictank0:6063 cass localhost:9042 raintank vegeta-render-patterns | vegeta attack -rate 10 -duration=2m | vegeta report
# TODO how to keep fake load going?
