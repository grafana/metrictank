#!/bin/bash
go run main.go | sort > out.txt

list=$GOPATH/src/github.com/raintank/raintank-tsdb-benchmark/results/metriclist.txt

echo "in ES:"
wc -l out.txt

echo "in metricslist (expected)"
wc -l $list
echo

diff <(sort $list) out.txt
