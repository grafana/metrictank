#!/bin/bash

echo "run this from inside a directory that has the mt-index-cat binary"
echo "before running this, make sure you run the following command and you saw the 'now doing realtime' message"
cat <<EOF
fakemetrics -carbon-tcp-address localhost:2003 -statsd-addr localhost:8125 -shard-org -keys-per-org 100 -speedup 200 -stop-at-now -offset 1h && echo "now doing realtime" && fakemetrics -carbon-tcp-address localhost:2003 -statsd-addr localhost:8125 -shard-org -keys-per-org 100
EOF

echo "and verify http://localhost:3000/dashboard/db/fake-metrics-data"
echo "press any key to continue"
read


function run () {
	local title="$1"
	local port="$2"
	local target="$3"
	local from="$4"
	local rate="$5"
	local duration="$6"
	local sleep="$7"

	echo ">>>> $title"
	./mt-index-cat -prefix 'some.id' -max-age 0 cass -hosts localhost:9042 "GET http://localhost:$port/render?target=$target&format=json&from=-$from\nX-Org-Id: 1\n\n" | vegeta attack -rate $rate -duration $duration | vegeta report
	sleep $sleep
	echo
}


run '1A: MT simple series requests'                      6060 '{{.Name}}'                      1h 10   50s 10s
run '1B: graphite simple series requests'                8080 '{{.Name}}'                      1h 10   50s 10s

run '2A MT sumSeries(patterns.*) (no proxying)'          6060 'sumSeries({{.Name | pattern}})' 1h 100  25s 5s
run '2B graphite sumSeries(patterns.*)'                  8080 'sumSeries({{.Name | pattern}})' 1h 100  25s 5s

run '3A MT load needing proxying'                        6060 'aliasByNode({{.Name}})'         1h 100  50s 10s
run '3B graphite directly to see proxying overhead'      8080 'aliasByNode({{.Name}})'         1h 100  50s 10s
