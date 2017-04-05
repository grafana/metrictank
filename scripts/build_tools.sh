#!/bin/bash
set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

GITVERSION=`git describe --always`
SOURCEDIR=${DIR}/..
BUILDDIR=$SOURCEDIR/build

# Make dir
mkdir -p $BUILDDIR

# disable cgo
export CGO_ENABLED=0

function fail () {
	echo "Aborting due to failure." >&2
	exit 2
}

# Build binary
cd $GOPATH/src/github.com/raintank/metrictank/cmd
for tool in *; do
  cd $tool
  if [ "$1" == "-race" ]
  then
    CGO_ENABLED=1 go build -race -ldflags "-X main.GitHash=$GITVERSION" -o $BUILDDIR/$tool || fail
  else
    go build -ldflags "-X main.GitHash=$GITVERSION" -o $BUILDDIR/$tool || fail
  fi
  cd ..
done
