#!/bin/bash

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/
source ../lib/util.sh
source lib.sh

docker_dir=../../docker/docker-cluster-query
cp storage-schemas.conf $docker_dir


echo "waiting til an even 2minutely timestamp to provide a fair starting point"
wait_time 120
cd $docker_dir
docker-compose up --force-recreate  -V -d
sleep 60
log "starting backfill..."
mt-fakemetrics backfill --kafka-mdm-addr localhost:9092 --offset 24h --period 10s --speedup 250 --mpo 2000
# looks like the backfill may write 4 minutes into the future because the first 4min of feed results in out of order drops. oh well...
log "backfill done. starting realtime feed..."
mt-fakemetrics feed --kafka-mdm-addr localhost:9092 --period 10s --mpo 2000 &

cd ../..

log "query 11 series over and over again, 50Hz"
same11

log "query 11 distinct series over and over again, 50Hz"
distinct11

log "pathological queries: one that queries ALL data (with MDP consolidation)"
pathological_all

log "pathological queries: one that queries ALL data (without MDP consolidation)"
pathological_all_no_mdp

log "pathological queries: ALL series but tiny responses"
pathological_all_tiny

log "anySeriesAnyTimeRange"
anySeriesAnyTimeRange

log "pathological-all + distinct11 simultaneously"
pathological_all &
distinct11

