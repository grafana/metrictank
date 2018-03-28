#!/bin/bash
# TODO. received stats point vs array, onfirm function of first MD then MP
# also test with/without snappy
# TODO: fakemetrics keycache expiration doesn't work

# memory analysis:
# problem with this is start stats are flakey. better start it 2 min earlier
# start stack at like x:19, so its ready 10 seconds before x:20
# start run at x:20 - don't issue ANY read queries to MT


#snap=120
#duration=400
snap=60
duration=200

function log () {
  echo "$(date +'%F %H:%M:%S') $1"
}

function wait_time() {
	sleep=$(( $snap - ($(date +%s) % $snap)))
	log "waiting $sleep seconds..."
	sleep $sleep
}

function stop() {
	docker stop $(docker ps | grep fakemetrics | awk '{print $1}')
}

stop

wait_time
log "START MDM-OLD"
docker run -d --rm --name=fakemetrics --net="host" fakemetrics /fakemetrics feed --kafka-mdm-addr localhost:9092 --mpo 25000 --kafka-mdm-v2=false --kafka-comp=snappy --add-tags=false
sleep $duration
stop

wait_time
log "START MDM-NEW"
docker run -d --rm --name=fakemetrics --net="host" fakemetrics /fakemetrics feed --kafka-mdm-addr localhost:9092 --mpo 25000 --kafka-mdm-v2=true --kafka-comp=snappy --add-tags=false
sleep $duration
stop

wait_time
log "START MDM-OLD"
docker run -d --rm --name=fakemetrics --net="host" fakemetrics /fakemetrics feed --kafka-mdm-addr localhost:9092 --mpo 25000 --kafka-mdm-v2=false --kafka-comp=snappy --add-tags=false
sleep $duration
stop

wait_time
log "START MDM-NEW"
docker run -d --rm --name=fakemetrics --net="host" fakemetrics /fakemetrics feed --kafka-mdm-addr localhost:9092 --mpo 25000 --kafka-mdm-v2=true --kafka-comp=snappy --add-tags=false
sleep $duration
stop
