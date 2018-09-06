#!/bin/bash
# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
go get -u github.com/fzipp/gocyclo
gocyclo -over 15 $(find . -name '*.go' | grep -v vendor | grep -v _gen.go)
