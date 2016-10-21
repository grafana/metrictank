#!/bin/sh

set -x # debugging

FAKEMETRICS_REPO="github.com/raintank/fakemetrics"
FAKEMETRICS_PID="/tmp/fakemetrics.pid"

if [ $# -eq 0 ]
then
  echo "$0 start|stop"
  exit 1
fi

ACTION=$1
METRICS_PER_SEC=${METRICS_PER_SEC:-1000}
HOST=${HOST:-"127.0.0.1"}
PORT=${PORT:-2003}

# ensure that $GOPATH is part of $PATH
GOBIN="$GOPATH/bin"
export PATH=$(echo "$PATH"| grep -q "$GOBIN" && echo "$PATH" || echo "$PATH:$GOBIN")

case "$ACTION" in
  "start")
    go get $FAKEMETRICS_REPO
    echo "generating test data..."
    fakemetrics -keys-per-org $METRICS_PER_SEC -orgs 1 --carbon-tcp-address $HOST:$PORT &
    echo $! > $FAKEMETRICS_PID
    ;;
  "stop")
    pkill -F $FAKEMETRICS_PID
    ;;
esac
