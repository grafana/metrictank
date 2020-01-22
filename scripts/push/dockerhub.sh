#!/bin/bash
set -e
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/../..
source scripts/version-tag.sh

echo
echo "### docker push grafana/metrictank:$version"
echo

docker push grafana/metrictank:$version

echo
echo "### docker push grafana/metrictank:$tag"
echo

docker push grafana/metrictank:$tag


echo
echo "### docker push grafana/mt-gateway:$version"
echo

docker push grafana/mt-gateway:$version

echo
echo "### docker push grafana/mt-gateway:$tag"
echo

docker push grafana/mt-gateway:$tag
