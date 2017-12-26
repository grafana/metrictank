#!/bin/bash
#TODO for some reason this doesn't work on circleci :+?
# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
go get -u github.com/gordonklaus/ineffassign
ineffassign .
