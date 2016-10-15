#!/bin/sh

FAKEMETRICS_REPO="github.com/raintank/fakemetrics"
FAKEMETRICS_PID="/tmp/fakemetrics.pid"

METRICS_PER_SEC=${METRICS_PER_SEC:-1000}
HOST=${HOST:-"127.0.0.1"}
PORT=${PORT:-2003}

go get github.com/raintank/fakemetrics

echo "generating test data..."
${GOPATH}/bin/fakemetrics -keys-per-org ${METRICS_PER_SEC} -orgs 1 --carbon-tcp-address ${HOST}:${PORT} &
