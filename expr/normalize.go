package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/util"
)

// Normalize normalizes series to the same common LCM interval - if they don't already have the same interval
// any adjusted series gets created in a series drawn out of the pool and is added to the cache so it can be reclaimed
func Normalize(cache map[Req][]models.Series, in []models.Series) []models.Series {
	var intervals []uint32
	for _, s := range in {
		if s.Interval == 0 {
			panic("illegal interval 0")
		}
		intervals = append(intervals, s.Interval)
	}
	lcm := util.Lcm(intervals)
	for i, s := range in {
		if s.Interval != lcm {
			in[i] = NormalizeTo(cache, s, lcm)
		}
	}
	return in
}

func NormalizeTwo(cache map[Req][]models.Series, a, b models.Series) (models.Series, models.Series) {
	if a.Interval == b.Interval {
		return a, b
	}
	intervals := []uint32{a.Interval, b.Interval}
	lcm := util.Lcm(intervals)

	if a.Interval != lcm {
		a = NormalizeTo(cache, a, lcm)
	}
	if b.Interval != lcm {
		b = NormalizeTo(cache, b, lcm)
	}
	return a, b
}

// NormalizeTo normalizes the given series to the desired interval
// the following MUST be true when calling this:
// * interval > in.Interval
// * interval % in.Interval == 0
func NormalizeTo(cache map[Req][]models.Series, in models.Series, interval uint32) models.Series {
	// we need to copy the datapoints first because the consolidater will reuse the input slice
	// also, the input may not be pre-canonical. so add nulls in front and at the back to make it pre-canonical.
	// this may make points in front and at the back less accurate when consolidated (e.g. summing when some of the points are null results in a lower value)
	// but this is what graphite does....
	datapoints := pointSlicePool.Get().([]schema.Point)

	if len(in.Datapoints) == 0 {
		panic(fmt.Sprintf("series %q cannot be normalized from interval %d to %d because it is empty", in.Target, in.Interval, interval))
	}

	// example of how this works:
	// if in.Interval is 5, and interval is 15, then for example, to generate point 15, you want inputs 5, 10 and 15.
	// or more generally (you can follow any example vertically):
	//  5 10 15 20 25 30 35 40 45 50 <-- if any of these timestamps are your first point in `in`
	//  5  5  5 20 20 20 35 35 35 50 <-- then these are the corresponding timestamps of the first values we want as input for the consolidator
	// 15 15 15 30 30 30 45 45 45 60 <-- which, when fed through alignForward(), result in these numbers
	//  5  5  5 20 20 20 35 35 35 50 <-- subtract (aggnum-1)* in.interval or equivalent -interval + in.Interval = -15 + 5 = -10. these are our desired numbers!

	for ts := alignForward(in.Datapoints[0].Ts, interval) - interval + in.Interval; ts < in.Datapoints[0].Ts; ts += interval {
		datapoints = append(datapoints, schema.Point{Val: math.NaN(), Ts: ts})
	}

	datapoints = append(datapoints, in.Datapoints...)

	in.Datapoints = consolidation.Consolidate(datapoints, interval/in.Interval, in.Consolidator)
	in.Interval = interval
	cache[Req{}] = append(cache[Req{}], in)
	return in
}

// alignForward aligns ts to the next timestamp that divides by the interval, except if it is already aligned
func alignForward(ts, interval uint32) uint32 {
	remain := ts % interval
	if remain == 0 {
		return ts
	}
	return ts + interval - remain
}
