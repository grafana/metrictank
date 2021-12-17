#!/bin/bash

# finds unchecked errors

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
echo $DIR
cd ${DIR}/../..
go install github.com/kisielk/errcheck@latest
export GO111MODULE=off
errcheck -ignoregenerated ./...
