#!/bin/bash
# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..

out=$(gofmt -d -s $(find . -name '*.go' | grep -v vendor | grep -v _gen.go))
if [ "$out" != "" ]; then
	echo "$out"
	echo
	echo "You might want to run something like 'find . -name '*.go' | xargs gofmt -w -s'"
	exit 2
fi
exit 0
