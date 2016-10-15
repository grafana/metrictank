#!/bin/sh

set -x # debugging

DOCKER_COMPOSE_FILE="docker/docker-compose.v2.yml"

# start all docker containers
docker-compose -f ${DOCKER_COMPOSE_FILE} up -d

# wait for carbon input before sending data
export WAIT_HOSTS="127.0.0.1:2003"
export WAIT_TIMEOUT=60
export METRICS_PER_SECOND=1000
scripts/wait_for_endpoint.sh scripts/generate_test_data.sh

# give fakemetrics some warmup time
sleep 30

# verify the metrics have arrived in graphite and return exit status
scripts/verify_metrics_received.py 127.0.0.1 8080 10 ${METRICS_PER_SECOND}
RESULT=${?}

exit  ${RESULT}
