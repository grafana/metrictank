#!/bin/bash

set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

VERSION=`git describe --always`

mkdir -p build
cp ../build/* build/

docker build -t raintank/metrictank .
docker tag raintank/metrictank raintank/metrictank:latest
docker tag raintank/metrictank raintank/metrictank:$VERSION

