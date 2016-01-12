#!/bin/bash

# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

: ${GOPATH:="${HOME}/.go_workspace"}
export PATH=$GOPATH/bin:$PATH

go get github.com/bmizerany/assert

for VAR in nsq_probe_events_to_elasticsearch metric_tank; do
	cd ${DIR}/../${VAR}
	go get ./...
	cd $DIR
	#go get -u -f github.com/raintank/raintank-metric/$VAR
	go install github.com/raintank/raintank-metric/$VAR
	cp $(which $VAR) ${DIR}/artifacts
done
