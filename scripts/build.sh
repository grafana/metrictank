#!/bin/bash

set -e

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/..

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

# disable cgo
export CGO_ENABLED=0

function fail () {
	echo "Aborting due to failure." >&2
	exit 2
}

# Build binary
cd cmd
for bin in *; do
  cd $bin
  if [ "$1" == "-race" ]
  then
    set -x
    CGO_ENABLED=1 go build -race -ldflags "-X main.gitHash=$GITVERSION" -o $BUILDDIR/$bin || fail
  else
    set -x
    go build -ldflags "-X main.gitHash=$GITVERSION" -o $BUILDDIR/$bin || fail
  fi
  set +x
  cd ..
done
