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

# Clean build bin dir
rm -rf $BUILDDIR/*

# Build binary
cd $GOPATH/src/github.com/raintank/metrictank
go build -ldflags "-X main.GitHash=$GITVERSION" -o $BUILDDIR/metrictank
