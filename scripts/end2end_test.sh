#!/bin/sh

set -x # debugging

DOCKER_COMPOSE_EXEC="/tmp/docker-compose"
DOCKER_COMPOSE_FILE="docker/docker-compose.v1.yml"
DOCKER_COMPOSE_VERSION="1.8.1"
curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-Linux-x86_64" > ${DOCKER_COMPOSE_EXEC}
chmod +x ${DOCKER_COMPOSE_EXEC}

# start all docker containers
export DOCKER_NETWORK="docker-net"
docker network create --driver bridge ${DOCKER_NETWORK}
${DOCKER_COMPOSE_EXEC} -f ${DOCKER_COMPOSE_FILE} up -d

# wait for carbon input before sending data
export WAIT_HOSTS="127.0.0.1:2003"
export WAIT_TIMEOUT=60
export METRICS_PER_SECOND=1000
scripts/wait_for_endpoint.sh scripts/generate_test_data.sh start

# give fakemetrics some warmup time
sleep 30

# verify the metrics have arrived in graphite and return exit status
scripts/verify_metrics_received.py 127.0.0.1 8080 10 ${METRICS_PER_SECOND}
RESULT=${?}

${DOCKER_COMPOSE_EXEC} -f ${DOCKER_COMPOSE_FILE} down
docker network rm ${DOCKER_NETWORK}
scripts/generate_test_data.sh stop

exit  ${RESULT}
