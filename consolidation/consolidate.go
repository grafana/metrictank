package consolidation

import (
	"context"

	"gopkg.in/raintank/schema.v1"
)

// ConsolidateContext wraps a Consolidate() call with a context.Context condition
func ConsolidateContext(ctx context.Context, in []schema.Point, aggNum uint32, consolidator Consolidator) []schema.Point {
	select {
	case <-ctx.Done():
		//request canceled
		return nil
	default:
	}
	return Consolidate(in, aggNum, consolidator)
}

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

// ConsolidateStable consolidates points in a "stable" way, meaning if you run the same function again so that the input
// receives new points at the end and old points get removed at the beginning, we keep picking the same points to consolidate together
// interval is the interval between the input points
func ConsolidateStable(points []schema.Point, interval, maxDataPoints uint32, consolidator Consolidator) ([]schema.Point, uint32) {
	aggNum := AggEvery(uint32(len(points)), maxDataPoints)
	// note that the amount of points to strip is always < 1 postAggInterval's worth.
	// there's 2 important considerations here:
	// 1) we shouldn't make any too drastic alterations of the timerange returned compared to the requested time range
	// 2) the stripping effort shouldn't significantly alter the output otherwise things get confusing
	// these 2 remarks boil down to "the amount of points stripped should be a small fraction of the amount of input points"
	// we use this simple heuristic:
	// only nudge if we have points > 2 * postAggInterval's worth where "postAggInterval's worth is aggNum points"
	//
	// this also assures that in the special case where people request MaxDataPoints=1 we will always consolidate
	// all points together and don't trim a significant amount of the points
	// that are expected to go into the aggregation
	// e.g. consider a case where we have points with ts 140,150,160,170
	// aggNum = aggEvery(4/1) = 4, postAggInterval is thus 40.
	// strict application of the logic would return 1 point with ts=200 (aggregation of all points 170-200 which is 1 point)
	// and strip the first 3 points,
	// which is not what we want. since we only have a small set of points, better to incorporate all points into 1 bucket with ts 170.
	// note that in this case (where we don't nudge) the timestamps in output are not cleanly divisible by postAggInterval

	// we only start stripping if we have more than 2*4=8 points
	// see the unit tests which explore cases like this (TestConsolidateStableNoTrimDueToNotManyPoints)
	if len(points) > int(2*aggNum) {
		_, num := nudge(points[0].Ts, interval, aggNum)
		points = points[num:]
	}
	points = Consolidate(points, aggNum, consolidator)
	interval *= aggNum
	return points, interval
}

// Nudge computes the parameters for nudging.
// let's say a series has points A,B,C,D and we must consolidate with numAgg=2.
// if we wait a step, point E appears into the visible window and A will slide out of the window.
// there's a few approaches you can take wrt such changes across refreshes:
// 1) naive old approach:
//    on first load return consolidate(A,B), consolidate(C,D)
//    after a step, return consolidate(B,C), consolidate(D,E)
//    => this looks weird across refreshes:
//       both the values as well as the timestamps change everywhere, points jump around on the chart
// 2) graphite-style nudging: trim a few of the first points away as needed, so that the first TS
//    is always a multiple of the postConsolidationInterval (note: assumes input is quantized!)
//    on first load return consolidate(A,B), consolidate(C,D)
//    after a step, return consolidate(C,D), consolidate(E)
//    => same points are always consolidated together, no jumping around.
//    => simple to understand, but not the most performant (fetches/processes some points needlessly)
//    => tends to introduce emptyness in graphs right after the requested start.
//       (because Grafana plots from requested start, not returned start, and so there will be some empty space
//       where we trimmed points)
// 3) similar to 2, but don't trim, rather consolidate the leftovers both on the start and at the end.
//    on first load return consolidate(A,B), consolidate(C,D)
//    after a step, return consolidate(B), consolidate(C,D), consolidate(E)
//    => same points are always consolidated together, no jumping around.
//    => only datapoint up front and at the end may jump around, but not the vast majority
//    => no discarding of points
//    => requires a large code change though, as it makes it harder to honor MaxDataPoints.
//       e.g. MDP=1, you have 5 points and aggNum is 5, if alignment is improper, it would cause
//       2 output points, so we would have to rewrite a lot of code, no longer compute AggNum in advance etc
//
// note that with all 3 approaches, we always consolidate leftovers at the end together, so with any approach
// the last point may jump around (see Consolidate function)
// for now, and for simplicity we just implement the 2nd approach. it's also the only one that assures MDP is strictly
// honored (see last point of approach 3, which also affects approach 1)
func nudge(start, preAggInterval, aggNum uint32) (uint32, int) {
	postAggInterval := preAggInterval * aggNum
	var num int
	var diff uint32
	// two important principles here:
	// 1) aggregation buckets have timestamps that are cleanly divisible by postAggInterval.
	// 2) we never make data pretend to be able to predict the future,
	//    e.g. when aggregating a spike, the spike should never move to an earlier timestamp
	//    in other words, data should only ever move to a later timestamp when being aggregated, never to the past.
	// it follows that the start of (first point going into) an aggregation bucket
	// is the point that has a timestamp of 1 preAggInterval after the cleanly-divisible-by-postAggInterval timestamp.

	// let's clarify all this with an example.
	// consider: preAggInterval = 5, aggNum = 4 => postAggInterval = 20
	// points marked with a * are the first points going into an aggregation interval

	// input   consolidate into    output points
	// 15      ▼
	// 20        ►                 20
	// 25 *    ▼
	// 30      ▼
	// 35      ▼
	// 40        ►                 40
	// 45 *    ▼
	// 50      ▼
	// 55      ▼
	// 60        ►                 60
	// 65 *    ▼
	// 70      ▼
	// 75      ▼
	//                             80

	// with our nudging approach, we strip all the points that do not form a full aggregation bucket.
	// IOW the desired first point must be the the first point of a bucket.
	// in the example above, it means we strip the first 2 points and start at ts=25 (which will go into ts=40).

	// move start until it maps to the first point of an aggregation bucket
	remainder := (start - preAggInterval) % postAggInterval
	if remainder > 0 {
		diff = postAggInterval - remainder
		num = int(diff / preAggInterval)
	}
	// following the above example:
	// remainder = (15 - 5) % 20 = 10
	// diff     = 20 - 10 = 10 // start will be moved up by 10s, from 15 to 25
	// num      = 10/5 = 2 // which means we will strip the first two points

	return diff, num
}
