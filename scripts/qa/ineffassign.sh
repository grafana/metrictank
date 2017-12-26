#!/bin/bash

# TODO for some reason this doesn't work on circleci :+?
# it doesn't find the problem on line 64 of idx.go

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
echo $DIR
cd ${DIR}/../..
pwd
go get -u -v github.com/dgnorton/ineffassign
cat -n idx/idx.go
echo "running now"
ineffassign -v .
rt=$?
echo "return code is $rt"
exit $rt
