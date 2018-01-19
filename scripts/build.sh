#!/bin/bash
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

# make sure CircleCI gets all tags properly.
# see https://discuss.circleci.com/t/where-are-my-git-tags/2371
git fetch --unshallow

GITVERSION=`git describe --always`
SOURCEDIR=${DIR}/..
BUILDDIR=$SOURCEDIR/build

# Make dir
mkdir -p $BUILDDIR

# Clean build bin dir
rm -rf $BUILDDIR/*

# disable cgo
export CGO_ENABLED=0

# Build binary
cd $GOPATH/src/github.com/grafana/metrictank

OUTPUT=$BUILDDIR/metrictank

if [ "$1" == "-race" ]
then
  set -x
  CGO_ENABLED=1 go build -race -ldflags "-X main.GitHash=$GITVERSION" -o $OUTPUT
else
  set -x
  go build -ldflags "-X main.GitHash=$GITVERSION" -o $OUTPUT
fi
