#!/bin/bash
# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
gopath=${GOPATH/:*/} # get the first dir

go get -u golang.org/x/tools/cmd/stringer github.com/tinylib/msgp

go generate $(go list ./... | grep -v /vendor/)
out=$(git status --short)
[ -z "$out" ] && echo "all good" && exit 0

echo "??????????????????????? Did you forget to run go generate ???????????????????"
echo "## git status after running go generate:"
git status
echo "## git diff after running go generate:"
# if we don't pipe into cat, this will just hang and timeout in circleCI
# I think because git tries to be smart and use an interactive pager,
# for which I could not find a nicer way to disable.
git diff | cat -

echo "You should probably run:"
echo "go get -u golang.org/x/tools/cmd/stringer github.com/tinylib/msgp"
echo 'go generate $(go list ./... | grep -v /vendor/)'
exit 2
