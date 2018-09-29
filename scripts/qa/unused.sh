#!/bin/bash

# runs the tools unused, varcheck and structcheck
# see their websites for more info.

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
go get -u honnef.co/go/tools/cmd/unused
go get -u github.com/opennota/check/cmd/varcheck
go get -u github.com/opennota/check/cmd/structcheck
# for https://github.com/remyoudompheng/go-misc/pull/14
go get -u github.com/Dieterbe/go-misc/deadcode

ret=0

echo "## running unused"
unused ./...
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
