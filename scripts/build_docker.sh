#!/bin/bash

set -x
set -e
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/..
source ${DIR}/version-tag.sh

# regular image
if [ -z "$1" ]
then
    docker build -t grafana/metrictank -f ${DIR}/Dockerfile .
    docker tag grafana/metrictank grafana/metrictank:$tag
    docker tag grafana/metrictank grafana/metrictank:$version

    # k8s image
    cd ${DIR}/k8s
    docker build -t us.gcr.io/metrictank-gcr/metrictank .
    docker tag us.gcr.io/metrictank-gcr/metrictank us.gcr.io/metrictank-gcr/metrictank:$tag
    docker tag us.gcr.io/metrictank-gcr/metrictank us.gcr.io/metrictank-gcr/metrictank:$version
elif [ "$1" == "-debug" ]
then
    # normal debug/dlv version
    docker build -t grafana/metrictank:latest-debug -f ${DIR}/dlv/Dockerfile .
    docker tag grafana/metrictank:latest-debug grafana/metrictank:${tag}-debug
    docker tag grafana/metrictank:latest-debug grafana/metrictank:${version}-debug

    # k8s debug/dlv version
    cd ${DIR}/k8s_dlv
    docker build -t us.gcr.io/metrictank-gcr/metrictank:latest-debug .
    docker tag us.gcr.io/metrictank-gcr/metrictank:latest-debug us.gcr.io/metrictank-gcr/metrictank:${tag}-debug
    docker tag us.gcr.io/metrictank-gcr/metrictank:latest-debug us.gcr.io/metrictank-gcr/metrictank:${version}-debug
else
    echo "Invalid argument supplied: ${1}"
    fail
fi
