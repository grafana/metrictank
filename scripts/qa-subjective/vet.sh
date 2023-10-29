#!/bin/bash

# runs all of the "go vet" checks

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..

go vet ./...
