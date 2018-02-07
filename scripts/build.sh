#!/bin/bash

set -x
# Find the directory we exist within
SCRIPTS_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
SOURCE_DIR=$SCRIPTS_DIR/..
BUILD_DIR=$SOURCE_DIR/build
TMP_DIR=$(mktemp -d)

cd $SOURCE_DIR

if ! [ -d $PKG_CONFIG_PATH ] || [ -z $PKG_CONFIG_PATH ]
then
	source scripts/build_deps.sh
else
	echo "not building librdkafka"
fi

# make sure CircleCI gets all tags properly.
# see https://discuss.circleci.com/t/where-are-my-git-tags/2371
# and https://stackoverflow.com/questions/37531605/how-to-test-if-git-repository-is-shallow
[ -f $(git rev-parse --git-dir)/shallow ] && git fetch --unshallow

GITVERSION=`git describe --abbrev=7`
BUILDDIR=$(pwd)/build

# Make dir
mkdir -p $BUILDDIR

# Clean build bin dir
rm -rf $BUILDDIR/*

# enable cgo
export CGO_ENABLED=1

OUTPUT=$BUILDDIR/metrictank

if [ "$1" == "-race" ]
then
  set -x
  go build -tags static -race -ldflags "-X main.gitHash=$GITVERSION" -o $OUTPUT
else
  set -x
  go build -tags static -ldflags "-X main.gitHash=$GITVERSION" -o $OUTPUT
fi
