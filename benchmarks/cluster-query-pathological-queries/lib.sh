
# 11 series at a time x 1h each, 50Hz
function same11 () {
echo -e 'GET http://localhost:6061/render?target=some.id.of.a.metric.123*&from=-1h\nX-Org-Id: 1\n\n' | vegeta attack -duration 60s | vegeta report
}

# 1 series at a time out of the same set of 11, 50Hz
function distinct11 () {
	for i in 123 1230 1231 1232 1233 1234 1235 1236 1237 1238 1239; do
		echo -e "GET http://localhost:6061/render?target=some.id.of.a.metric.$i*&from=-1h\nX-Org-Id: 1\n\n"
	done | vegeta attack -duration 60s | vegeta report
}

# 2k series x 1440 points each fetched = 2.88M fetched, due to MDP default 800 -> 1.440 returned  total
# comes out of RAM+cassandra, rate 0.2Hz
function pathological_all () {
	echo -e 'GET http://localhost:6061/render?target=some.id.of.a.metric.*&from=-24h\nX-Org-Id: 1\n\n'  | vegeta attack -duration 60s -rate '1/5s' | vegeta report
}

# 2k series x 1440 points each fetched = 2.88M fetched+returned
# comes out of RAM+cassandra, rate 0.2Hz
function pathological_all_no_mdp () {
	echo -e 'GET http://localhost:6061/render?target=some.id.of.a.metric.*&maxDataPoints=1500&from=-24h\nX-Org-Id: 1\n\n'  | vegeta attack -duration 60s -rate '1/5s' | vegeta report
}

# 2k series x 6 points each = 12k fetched+returned
# comes out of RAM
# <100ms response each on dieter's laptop
# rate 0.5Hz
function pathological_all_tiny () {
	echo -e 'GET http://localhost:6061/render?target=some.id.of.a.metric.*&from=-60s\nX-Org-Id: 1\n\n'  | vegeta attack -duration 60s -rate '1/2s' | vegeta report
}


# any timeseries, any timerange of x hours in the last 24 hours. on avg 12h
# rate 0.5Hz
function anySeriesAnyTimeRange () {
	everySeriesEveryTimeRange | sort -R | vegeta attack -duration 60s -rate '1/2s' | vegeta report
}

# everyof the 2k series, and all time rages of x hours in the last 24 hours. on avg 12h
function everySeriesEveryTimeRange () {
for m in {1..2000}; do
	# increment timeranges in 1h ranges
	for start in {1..24}; do
		maxEnd=$((start-1))
		for end in `seq 0 $maxEnd`; do
			if [ "$end" -eq 0 ]; then
				until=now
			else
				until="-${end}h"
			fi
			echo -e "GET http://localhost:6061/render?target=some.id.of.a.metric.$m*&from=-${start}h&until=$until\nX-Org-Id: 1\n\n"
		done
	done
done
}

# could not get mt-index-cat to work... should be something like:
# ./build/mt-index-cat -addr http://localhost:6061 -from 60min -regex 'some\.id\.of\.a\.metric\.9....' cass -hosts localhost:9042 -schema-file scripts/config/schema-idx-cassandra.toml 'GET http://localhost:6061/render?target={{.Name | patternCustom 100 "1rccw" }}&from=-1h\nX-Org-Id: 1\n\n'


