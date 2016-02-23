#!/bin/bash

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

: ${GOPATH:="${HOME}/.go_workspace"}
export PATH=$GOPATH/bin:$PATH
GIT_HASH=`git rev-parse HEAD`

for VAR in nsq_probe_events_to_elasticsearch metric_tank; do
	go install -ldflags "-X main.GitHash $GIT_HASH" github.com/raintank/raintank-metric/$VAR
	cp $(which $VAR) ${DIR}/artifacts
done
