#!/bin/bash
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/../..

VERSION=`git describe --always`

docker login -u _json_key -p "$GOOGLE_AUTH" https://us.gcr.io
docker push us.gcr.io/metrictank-gcr/metrictank:latest
docker push us.gcr.io/metrictank-gcr/metrictank:$VERSION
