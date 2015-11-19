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

mkdir -p ${GOPATH}/src/github.com/grafana/grafana
mv ${GOPATH}/src/github.com/grafana/grafana ${GOPATH}/src/github.com/grafana/grafana-bak
mkdir -p ${GOPATH}/src/github.com/raintank
cd ${GOPATH}/src/github.com/raintank
git clone https://github.com/raintank/grafana.git
#echo "raintank"
#ls ${GOPATH}/src/github.com/raintank
#echo "grafana"
#ls ${GOPATH}/src/github.com/grafana
ln -s ${GOPATH}/src/github.com/raintank/grafana ${GOPATH}/src/github.com/grafana/grafana
# Only until this is done being in a different branch
cd ${GOPATH}/src/github.com/raintank/grafana

# link our code to our gopath.
ln -s $CHECKOUT $GOPATH/src/github.com/raintank/raintank-metric

# it, erm, seems to not be finding all of the dependencies right now
go get github.com/nsqio/go-nsq
go get github.com/tinylib/msgp/msgp
go get github.com/ctdk/goas/v2/logger
go get github.com/jessevdk/go-flags
go get github.com/BurntSushi/toml
go get github.com/mattbaird/elastigo/lib
go get gopkg.in/redis.v2
go get github.com/bitly/go-hostpool
go get github.com/marpaia/graphite-golang
go get github.com/codeskyblue/go-uuid
go get github.com/rakyll/globalconf
