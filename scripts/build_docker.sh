#!/bin/bash

set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

VERSION=`git describe --always`

rm -rf build/*
mkdir -p build
cp ../build/* build/

docker build -t grafana/metrictank .
docker tag grafana/metrictank grafana/metrictank:latest
docker tag grafana/metrictank grafana/metrictank:$VERSION

