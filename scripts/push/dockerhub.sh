#!/bin/bash
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/../..

VERSION=`git describe --abbrev=7`

echo
echo "### docker push grafana/metrictank:$VERSION"
echo

docker push grafana/metrictank:$VERSION || exit 2

echo
echo "### docker push grafana/metrictank:latest"
echo

docker push grafana/metrictank:latest
