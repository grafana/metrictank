#!/bin/bash

# checks for misspelled words

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
go install github.com/client9/misspell/cmd/misspell@latest
misspell -error $(find . -type f | grep -v vendor | grep -v "go\." | grep -v '.git' | grep -v Gopkg.lock)
