#!/bin/bash

# this script checks if running go-generate results in a git diff.
# if so, you forgot to run go generate (using the latest tools)
# note: if you already have dirty code changes before running this tool, it has no choice but to bail out
# but in CI mode, run with '-f' (--force) flag, which will always apply the check, as the working copy should always be clean

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
gopath=${GOPATH/:*/} # get the first dir

if [ "$1" != "-f" -a "$1" != "--force" ]; then
	out=$(git status --short)
	[ -n "$out" ] && echo "WARNING: working copy dirty. cannot accurately operate. skipping test" && exit 0
fi

go get -u golang.org/x/tools/cmd/stringer github.com/tinylib/msgp

go generate ./...
out=$(git status --short)
[ -z "$out" ] && echo "all good" && exit 0

echo "??????????????????????? Did you forget to run go generate ???????????????????"
echo "## git status after running go generate:"
git status
echo "## git diff after running go generate:"
# disable pager, otherwise this will just hang and timeout in circleCI
git --no-pager diff 

echo "You should probably run:"
echo "go get -u golang.org/x/tools/cmd/stringer github.com/tinylib/msgp"
echo 'go generate ./...'
exit 2
