#!/bin/bash

set -x
set -e
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}
source version-tag.sh

# regular image
rm -rf build/*
mkdir -p build
cp ../build/* build/

if [ -z "$1" ]
then
    docker build -t grafana/metrictank .
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
    docker build -t grafana/metrictank-debug -f ${DIR}/dlv/Dockerfile .
    docker tag grafana/metrictank-debug grafana/metrictank-debug:$tag
    docker tag grafana/metrictank-debug grafana/metrictank-debug:$version

    # k8s debug/dlv version
    cd ${DIR}/k8s_dlv
    docker build -t us.gcr.io/metrictank-gcr/metrictank-debug .
    docker tag us.gcr.io/metrictank-gcr/metrictank-debug us.gcr.io/metrictank-gcr/metrictank-debug:$tag
    docker tag us.gcr.io/metrictank-gcr/metrictank-debug us.gcr.io/metrictank-gcr/metrictank-debug:$version
else
    echo "Invalid argument supplied: ${1}"
    fail
fi
