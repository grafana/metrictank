#!/bin/bash

log () {
  echo "$(date +'%F %T') $1"
}

# converts a number of ns to a specificaton for `sleep`
ns_to_sleep() {
	local ns=$1
	printf "%010d\n" $ns | sed 's/\(.\{9\}\)$/.\1/'
}

# sleep until the next round timestamp (snap), but offsetted
# eg wait_time 60 7 will sleep until 7 seconds after the next whole minute
# obviously this is isn't accurate to the ns, due to overhead of executing commands, etc
# on Dieter's laptop, there is typically 5.7 to 5.8 millis of overhead, hence the compensation
wait_time() {
	local snap=$(($1 * 1000000000))
	local offset=$((${2:-0} * 1000000000))
	now=$(date +%s%N)
	sleep_ns=$(($snap - (($now + 5700000 - $offset) % $snap)))
	sleep $(ns_to_sleep $sleep_ns)
}
