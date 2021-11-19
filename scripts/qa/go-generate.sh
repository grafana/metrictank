#!/bin/bash

# this script checks if running go-generate results in a git diff.
# if so, you forgot to run go generate.
# note: if you already have dirty code changes before running this tool, it has no choice but to bail out
# but in CI mode, run with '-f' (--force) flag, which will always apply the check, as the working copy should always be clean

# lock the msgp tool to the same version as the vendored code, so the generated code is definitely compatible.
# the generated code also tends to receive small tweaks over time, we wouldn't want our build to suddenly break
# when that happens
msgpVersion=v1.1.0
stringerVersion=master

# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
gopath=${GOPATH/:*/} # get the first dir

if [ "$1" != "-f" -a "$1" != "--force" ]; then
	out=$(git status --short)
	[ -n "$out" ] && echo "WARNING: working copy dirty. cannot accurately operate. skipping test" && exit 0
fi


go install golang.org/x/tools/cmd/stringer@$stringerVersion
go install github.com/tinylib/msgp@$msgpVersion

cd -

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
echo "go install golang.org/x/tools/cmd/stringer@$stringerVersion"
echo "go install github.com/tinylib/msgp@$msgpVersion"
echo 'go generate ./...'
exit 2
