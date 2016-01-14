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


# link our checked out code to our gopath.
rm -rf $GOPATH/src/github.com/raintank/raintank-metric
ln -s $(dirname $(readlink -e $CHECKOUT)) $GOPATH/src/github.com/raintank/raintank-metric


for VAR in nsq_probe_events_to_elasticsearch metric_tank; do
	cd $GOPATH/src/github.com/raintank/raintank-metric
	go get -t -d ./...
	cd ${DIR}
done