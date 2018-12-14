#!/bin/bash
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/../..
source scripts/version-tag.sh

echo
echo "### docker login to us.gcr.io"
echo

docker login -u _json_key -p "$GOOGLE_AUTH" https://us.gcr.io

echo
echo "### docker push us.gcr.io/metrictank-gcr/metrictank:$version"
echo

docker push us.gcr.io/metrictank-gcr/metrictank:$version || exit 2

echo
echo "### docker push us.gcr.io/metrictank-gcr/metrictank:$tag"
echo

docker push us.gcr.io/metrictank-gcr/metrictank:$tag
