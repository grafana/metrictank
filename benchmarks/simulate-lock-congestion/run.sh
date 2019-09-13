#!/bin/bash

# Find the directory we exist within and cd to root dir of project
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/../..
pwd

for writeq in true false; do

	echo "# add-pct mean median p95 p99" > writeq-$writeq-metric-add.txt
	echo "# add-pct mean median p95 p99" > writeq-$writeq-metric-update.txt
	echo "# add-pct mean median p95 p99" > writeq-$writeq-query.txt

	for newpct in 5 10 25 35 50 75 85 100; do
		echo running experiment writeq-$writeq-newpct-$newpct
		filename=experiment-writeq-$writeq-newpct-$newpct.txt

		export GOMAXPROCS=8
		build/mt-simulate-lock-congestion\
			-new-series-percent $newpct\
			-add-threads 8\
			-adds-per-sec 8000\
			-initial-index-size 4000000\
			-series-file test_data/idx_lines\
			-queries-per-sec 80\
			-run-duration 120s\
			-queries-file test_data/query_patterns\
			memory-idx -write-queue-enabled=$writeq\
			-write-queue-delay=10s\
			-find-cache-backoff-time=10s &>> $filename

		if [ $? -gt 0 ]; then
			echo ABORT >&2
			exit $?
		fi
		read _ count mean median p95 p99 max < <(grep '^metric-add' $filename)
		echo "$newpct $mean $median $p95 $p99" >> writeq-$writeq-metric-add.txt
		read _ count mean median p95 p99 max < <(grep '^metric-update' $filename)
		echo "$newpct $mean $median $p95 $p99" >> writeq-$writeq-metric-update.txt
		read _ count mean median p95 p99 max < <(grep '^query' $filename)
		echo "$newpct $mean $median $p95 $p99" >> writeq-$writeq-query.txt
	done
done
