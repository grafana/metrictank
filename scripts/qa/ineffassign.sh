#!/bin/bash

# this tool detects "ineffectual assignments"
# not very clear but you may find some more info on
# https://github.com/gordonklaus/ineffassign

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
go get -u github.com/gordonklaus/ineffassign
ineffassign .
