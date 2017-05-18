package consolidation

import (
	"gopkg.in/raintank/schema.v1"
)

func Consolidate(in []schema.Point, aggNum uint32, consolidator Consolidator) []schema.Point {
	num := int(aggNum)
	aggFunc := GetAggFunc(consolidator)
	outLen := len(in) / num
	var out []schema.Point
	cleanLen := num * outLen // what the len of input slice would be if it was a perfect fit
	if len(in) == cleanLen {
		out = in[0:outLen]
		out_i := 0
		var next_i int
		for in_i := 0; in_i < cleanLen; in_i = next_i {
			next_i = in_i + num
			out[out_i] = schema.Point{Val: aggFunc(in[in_i:next_i]), Ts: in[next_i-1].Ts}
			out_i += 1
		}
	} else {
		outLen += 1
		out = in[0:outLen]
		out_i := 0
		var next_i int
		for in_i := 0; in_i < cleanLen; in_i = next_i {
			next_i = in_i + num
			out[out_i] = schema.Point{Val: aggFunc(in[in_i:next_i]), Ts: in[next_i-1].Ts}
			out_i += 1
		}
		// we have some leftover points that didn't get aggregated yet
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
		out[out_i] = schema.Point{Val: aggFunc(in[cleanLen:]), Ts: lastTs}
	}
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
