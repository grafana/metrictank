#!/bin/bash

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

#: ${GOPATH:="${HOME}/.go_workspace"}

if [ ! -z ${CIRCLECI} ] ; then
  : ${CHECKOUT:="/home/ubuntu/${CIRCLE_PROJECT_REPONAME}"}
else
  : ${CHECKOUT:="${DIR}/.."}
fi

export PATH=$GOPATH/bin:$PATH

mkdir -p artifacts
bundle install

mkdir ${GOPATH}/src/github.com/raintank
ln -s $CHECKOUT $GOPATH/src/github.com/raintank/raintank-metric

mkdir ${GOPATH}/src/github.com/grafana
cd ${GOPATH}/src/github.com/raintank
git clone https://github.com/raintank/grafana.git
ln -s ${GOPATH}/src/github.com/raintank/grafana ${GOPATH}/src/grafana/grafana
# Only until this is done being in a different branch

go get -u -f github.com/raintank/raintank-metric
go get github.com/bitly/go-nsq
go get github.com/tinylib/msgp/msgp
