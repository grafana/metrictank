#!/bin/bash
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/../..

VERSION=`git describe --always`

echo docker push grafana/metrictank:$VERSION
docker push grafana/metrictank:$VERSION || exit 2
echo docker push grafana/metrictank:latest
docker push grafana/metrictank:latest
