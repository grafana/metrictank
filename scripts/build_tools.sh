#!/bin/bash
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/..

GITVERSION=`git describe --abbrev=7`
BUILDDIR=$(pwd)/build

# Make dir
mkdir -p $BUILDDIR

# enable cgo
export CGO_ENABLED=1

function fail () {
	echo "Aborting due to failure." >&2
	exit 2
}

# Build binary
cd cmd
for tool in *; do
  cd $tool
  if [ "$1" == "-race" ]
  then
    set -x
    go build -tags static -race -ldflags "-X main.gitHash=$GITVERSION" -o $BUILDDIR/$tool || fail
  else
    set -x
    go build -tags static -ldflags "-X main.gitHash=$GITVERSION" -o $BUILDDIR/$tool || fail
  fi
  set +x
  cd ..
done
