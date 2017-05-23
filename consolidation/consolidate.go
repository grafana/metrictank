package consolidation

import (
	"gopkg.in/raintank/schema.v1"
)

// Consolidate consolidates `in`, aggNum points at a time via the given function
// note: the returned slice repurposes in's backing array.
func Consolidate(in []schema.Point, aggNum uint32, consolidator Consolidator) []schema.Point {
	num := int(aggNum)
	aggFunc := GetAggFunc(consolidator)

	// let's see if the input data is a perfect fit for the requested aggNum
	// (e.g. no remainder). This case is the easiest to handle
	outLen := len(in) / num
	cleanLen := num * outLen
	if len(in) == cleanLen {
		out := in[0:outLen]
		var outI, nextI int
		for inI := 0; inI < cleanLen; inI = nextI {
			nextI = inI + num
			out[outI] = schema.Point{Val: aggFunc(in[inI:nextI]), Ts: in[nextI-1].Ts}
			outI += 1
		}
		return out
	}

	// the fit is not perfect: first process all the aggNum sized groups:
	outLen += 1
	out := in[0:outLen]
	var outI, nextI int
	for inI := 0; inI < cleanLen; inI = nextI {
		nextI = inI + num
		out[outI] = schema.Point{Val: aggFunc(in[inI:nextI]), Ts: in[nextI-1].Ts}
		outI += 1
	}

	// we have some leftover points that didn't get aggregated yet because they're fewer than aggNum.
	// we must also aggregate it and add it, and the timestamp of this point must be what it would have been
	// if the group would have been complete, i.e. points in the consolidation output should be evenly spaced.
	// obviously we can only figure out the interval if we have at least 2 points
	var lastTs uint32
	if len(in) == 1 {
		lastTs = in[0].Ts
	} else {
		interval := in[len(in)-1].Ts - in[len(in)-2].Ts
		// len 10, cleanLen 9, num 3 -> 3*4 values supposedly -> "in[11].Ts" -> in[9].Ts + 2*interval
		lastTs = in[cleanLen].Ts + (aggNum-1)*interval
	}
	out[outI] = schema.Point{Val: aggFunc(in[cleanLen:]), Ts: lastTs}
	return out
}

// returns how many points should be aggregated together so that you end up with as many points as possible,
// but never more than maxPoints
func AggEvery(numPoints, maxPoints uint32) uint32 {
	if numPoints == 0 {
		return 1
	}
	return (numPoints + maxPoints - 1) / maxPoints
}
