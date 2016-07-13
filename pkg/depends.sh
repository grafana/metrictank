#!/bin/bash

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

: ${GOPATH:="${HOME}/.go_workspace"}

if [ ! -z ${CIRCLECI} ] ; then
  : ${CHECKOUT:="/home/ubuntu/${CIRCLE_PROJECT_REPONAME}"}
else
  : ${CHECKOUT:="${DIR}/.."}
fi

export PATH=$GOPATH/bin:$PATH

mkdir -p artifacts
bundle install

MYGOPATH=""
# find a writeable GOPATH
echo "searching for writeable GOPATH"
for p in ${GOPATH//:/ }; do 
	echo "checking if $p is writeable"
	if [ -w $p ]; then
		echo "using $p"
		MYGOPATH=$p
		break
	fi
done	

if [ -z ${MYGOPATH} ]; then
	echo "no writable GOPATH found."
	exit 1
fi

mkdir -p $MYGOPATH/src/github.com/raintank
rm -rf $MYGOPATH/src/github.com/raintank/metrictank

# link our checked out code to our gopath.
ABS_CHECKOUT=$(readlink -e $CHECKOUT)
ln -s $ABS_CHECKOUT ${MYGOPATH}/src/github.com/raintank/metrictank

cd ${MYGOPATH}/src/github.com/raintank/metrictank
go get -t -d ./...
cd ${DIR}
