#!/bin/sh

set -x # debugging

DOCKER_COMPOSE_FILE="docker/docker-dev/docker-compose.yml"

# start all docker containers
docker-compose -f $DOCKER_COMPOSE_FILE up -d

# wait for carbon input before sending data
export WAIT_HOSTS="127.0.0.1:2003"
export WAIT_TIMEOUT=120
export METRICS_PER_SECOND=1000
scripts/util/wait_for_endpoint.sh scripts/qa/generate_test_data.sh start

# give fakemetrics some warmup time
sleep 30

# verify the metrics have arrived in graphite and keep exit status
scripts/qa/verify_metrics_received.py 127.0.0.1 8080 10 $METRICS_PER_SECOND
RESULT=$?

scripts/qa/generate_test_data.sh stop
docker-compose -f $DOCKER_COMPOSE_FILE down

exit $RESULT
