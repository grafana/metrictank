#!/bin/bash
trap ctrl_c INT

function ctrl_c() {
    docker-compose down
}

../extra/populate-grafana.sh "$(basename $(pwd))" &
docker-compose up --force-recreate
