#!/bin/bash


function gen_input() {
	# we have 500 series and want to select 10 out of them
	for i in {1..300};
	do
		patt="$(($RANDOM % 5))$(($RANDOM % 10))*"
		echo "GET http://localhost:6061/render?target=some.id.of.a.metric.$patt;some=tag&from=-2y"
	done
}

input=$(gen_input)
echo "$input" | vegeta attack -rate 1 -duration 30s > exploration-out
