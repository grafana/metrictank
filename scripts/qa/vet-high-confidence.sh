#!/bin/bash
# go vet has high confidence checks and low confidence ones.
# for the purpose of CI, we shall only execute the high confidence ones.
# we can just follow the ones `go test` uses, it's not documented, but
# see https://github.com/golang/go/blob/release-branch.go1.10/src/cmd/go/internal/test/test.go#L509-L533
# NOTE: why not just rely on the auto go vet execution via go test -> because not all packages use go test

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..

go vet -atomic -bool -buildtags -nilfunc -printf ./...
