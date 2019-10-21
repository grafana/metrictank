#!/bin/bash

# runs the tools staticcheck, varcheck, structcheck and deadcode
# see their websites for more info.
# staticcheck is temporarily commented because of a recent commit which makes it segfault
# as soon as it is fixed, please uncomment again

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
#go get -u honnef.co/go/tools/cmd/staticcheck
go get -u github.com/opennota/check/cmd/varcheck
go get -u github.com/opennota/check/cmd/structcheck
# for https://github.com/remyoudompheng/go-misc/pull/14
go get -u github.com/Dieterbe/go-misc/deadcode

ret=0

#echo "## running staticcheck"
#staticcheck -checks U1000 ./...
#r=$?
#[ $r -gt $ret ] && ret=$r

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
