#!/bin/sh

FAKEMETRICS_REPO="github.com/raintank/fakemetrics"
FAKEMETRICS_PID="/tmp/fakemetrics.pid"

ACTION=${1}
METRICS_PER_SEC=${METRICS_PER_SEC:-1000}
HOST=${HOST:-"127.0.0.1"}
PORT=${PORT:-2003}

case ${ACTION} in
  "start")
    go get github.com/raintank/fakemetrics
    go run ../fakemetrics/fakemetrics.go -keys-per-org ${METRICS_PER_SEC} -orgs 1 --carbon-tcp-address ${HOST}:${PORT} &
    echo ${!} > ${FAKEMETRICS_PID}
    ;;
  "stop")
    # kill child of `go run` process, then parent should exit
    kill `pgrep -P $(cat ${FAKEMETRICS_PID})`
    ;;
  *)
    echo "${0} start|stop"
    ;;
esac
