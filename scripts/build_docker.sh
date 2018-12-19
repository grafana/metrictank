#!/bin/bash

set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}
source version-tag.sh

# regular image
rm -rf build/*
mkdir -p build
cp ../build/* build/

docker build -t grafana/metrictank .
docker tag grafana/metrictank grafana/metrictank:$tag
docker tag grafana/metrictank grafana/metrictank:$version

# k8s image
cd ${DIR}/k8s
docker build -t us.gcr.io/metrictank-gcr/metrictank .
docker tag us.gcr.io/metrictank-gcr/metrictank us.gcr.io/metrictank-gcr/metrictank:$tag
docker tag us.gcr.io/metrictank-gcr/metrictank us.gcr.io/metrictank-gcr/metrictank:$version
