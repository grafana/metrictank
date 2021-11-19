#!/bin/bash

# finds all kinds of lint

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
echo $DIR
cd ${DIR}/../..
go install golang.org/x/lint/golint@latest
export GO111MODULE=off
golint $(go list ./... | grep -v vendor)
