#!/bin/bash
# find the dir we exist within...
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# and cd into root project dir
cd ${DIR}/../..
gopath=${GOPATH/:*/} # get the first dir

go get -u golang.org/x/tools/cmd/stringer github.com/tinylib/msgp

# stringer needs all packages to be installed to work
# see https://github.com/golang/go/issues/10249
#go install ./...
function install () {
	local path=$1
	echo installing vendor/$path
	cd vendor/$path
	if ! go install .; then
		echo "installation of $path failed" >&2
		exit 2
	fi
	cd - >/dev/null

	desired=$gopath/pkg/linux_amd64/$path
	echo "let's see if the desired path '$desired' exists:"
	ls -alh $desired

	got=$gopath/pkg/linux_amd64/github.com/grafana/metrictank/vendor/$path
	echo "let see if the path we probably got '$got' exists:"
	ls -alh $got

	echo "assuming go put them in 'got', not 'desired', symlinking..."
	parent=$(dirname $desired)
	echo mkdir -p $parent
	mkdir -p $parent
	ln -s $got $desired
}

install github.com/opentracing/opentracing-go
install github.com/dgryski/go-tsz
install gopkg.in/raintank/schema.v1


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
