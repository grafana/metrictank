#!/bin/bash

# runs the tools staticcheck, varcheck, structcheck and deadcode
# see their websites for more info.

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
go install honnef.co/go/tools/cmd/staticcheck@latest
go install github.com/opennota/check/cmd/varcheck@latest
go install github.com/opennota/check/cmd/structcheck@latest
# for https://github.com/remyoudompheng/go-misc/pull/14
go install github.com/Dieterbe/go-misc/deadcode@latest

ret=0

export GO111MODULE=off

echo "## running staticcheck"
staticcheck -checks U1000 ./...
r=$?
[ $r -gt $ret ] && ret=$r

echo "## running varcheck"
varcheck ./...
r=$?
[ $r -gt $ret ] && ret=$r

echo "## running structcheck"
structcheck ./...
r=$?
[ $r -gt $ret ] && ret=$r

echo "## running deadcode"
deadcode -test $(find . -type d | grep -v '.git' | grep -v vendor | grep -v docker)
r=$?
[ $r -gt $ret ] && ret=$r

[ $ret -eq 0 ] && echo "all good"
exit $ret
