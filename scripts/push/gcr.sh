#!/bin/bash
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/../..

VERSION=`git describe --abbrev=7`

echo
echo "### docker login to us.gcr.io"
echo

docker login -u _json_key -p "$GOOGLE_AUTH" https://us.gcr.io

echo
echo "### docker push us.gcr.io/metrictank-gcr/metrictank:$VERSION"
echo

docker push us.gcr.io/metrictank-gcr/metrictank:$VERSION || exit 2

echo
echo "### docker push us.gcr.io/metrictank-gcr/metrictank:latest"
echo

docker push us.gcr.io/metrictank-gcr/metrictank:latest
