#!/bin/bash
go get -u golang.org/x/tools/cmd/stringer github.com/tinylib/msgp

cd $GOPATH/src/github.com/raintank/metrictank/
# stringer needs all packages to be installed to work
# see https://github.com/golang/go/issues/10249
#go install ./...
govendor install github.com/hashicorp/memberlist github.com/dgryski/go-tsz 
go generate $(go list ./... | grep -v /vendor/)
out=$(git status --short)
if [ -n "$out" ]; then
	echo "??????????????????????? Did you forget to run go generate ???????????????????" >&2
	echo "## git status after running go generate:" >&2
	git status >&2
	echo "## git diff after running go generate:" >&2
	# if we don't pipe into cat, this will just hang and timeout in circleCI
	# I think because git tries to be smart and use an interactive pager,
	# for which I could not find a nicer way to disable.
	git diff | cat - >&2

	echo >&2
	echo "You should probably run 'go generate \$(go list ./... | grep -v /vendor/)'" >&2
	exit 2
fi
exit 0
