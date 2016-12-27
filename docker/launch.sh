#!/bin/bash

basedir=$(dirname "$0")
env="$1"

function knownEnvs () {
	echo -e "Known environments:\n" >&2
	cd $basedir
	ls -1d docker-* >&2
}

if [[ ! "$env" =~ ^docker- ]]; then
	echo "env must start with docker-" >&2
	knownEnvs
	exit 1
fi

if [ ! -d "$basedir/$env" ]; then
	echo -e "Could not find docker environment $env\n" >&2
	knownEnvs
	exit 1
fi

cd $basedir/$env

trap ctrl_c INT

function ctrl_c() {
    docker-compose down
}

docker-compose down
../extra/populate-grafana.sh $PWD &
docker-compose up --force-recreate
