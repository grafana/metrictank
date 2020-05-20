package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/util"
	"github.com/grafana/metrictank/util/align"
)

// Normalize normalizes series to the same common LCM interval - if they don't already have the same interval
// any adjusted series gets created in a series drawn out of the pool and is added to the dataMap so it can be reclaimed
func Normalize(dataMap DataMap, in []models.Series) []models.Series {
	if len(in) < 2 {
		return in
	}
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
			in[i] = NormalizeTo(dataMap, s, lcm)
		}
	}
	return in
}

func NormalizeTwo(dataMap DataMap, a, b models.Series) (models.Series, models.Series) {
	if a.Interval == b.Interval {
		return a, b
	}
	intervals := []uint32{a.Interval, b.Interval}
	lcm := util.Lcm(intervals)

	if a.Interval != lcm {
		a = NormalizeTo(dataMap, a, lcm)
	}
	if b.Interval != lcm {
		b = NormalizeTo(dataMap, b, lcm)
	}
	return a, b
}

// NormalizeTo normalizes the given series to the desired interval
// the following MUST be true when calling this:
// * interval > in.Interval
// * interval % in.Interval == 0
func NormalizeTo(dataMap DataMap, in models.Series, interval uint32) models.Series {

	if len(in.Datapoints) == 0 {
		panic(fmt.Sprintf("series %q cannot be normalized from interval %d to %d because it is empty", in.Target, in.Interval, interval))
	}

	// we need to copy the datapoints first because the consolidater will reuse the input slice
	// also, the input may not be pre-canonical. so add nulls in front and at the back to make it pre-canonical.
	// this may make points in front and at the back less accurate when consolidated (e.g. summing when some of the points are null results in a lower value)
	// but this is what graphite does....
	datapoints := pointSlicePool.Get().([]schema.Point)

	// example of how this works:
	// if in.Interval is 5, and interval is 15, then for example, to generate point 15, you want inputs 5, 10 and 15.
	// or more generally (you can follow any example vertically):
	//  5 10 15 20 25 30 35 40 45 50 <-- if any of these timestamps are your first point in `in`
	//  5  5  5 20 20 20 35 35 35 50 <-- then these are the corresponding timestamps of the first values we want as input for the consolidator
	// 15 15 15 30 30 30 45 45 45 60 <-- which, when fed through alignForwardIfNotAligned(), result in these numbers
	//  5  5  5 20 20 20 35 35 35 50 <-- subtract (aggnum-1)* in.interval or equivalent -interval + in.Interval = -15 + 5 = -10. these are our desired numbers!

	// now, for the final value, it's important to be aware of cases like this:
	// until=47, interval=10, in.Interval = 5
	// a canonical 10s series would have as last point 40. whereas our input series will have 45, which will consolidate into a point with timestamp 50, which is incorrect
	// (it breaches `to`, and may have more points than other series it needs to be combined with)
	// thus, we also need to potentially trim points from the back until the last point has the same Ts as a canonical series would

	canonicalStart := align.ForwardIfNotAligned(in.Datapoints[0].Ts, interval) - interval + in.Interval
	for ts := canonicalStart; ts < in.Datapoints[0].Ts; ts += in.Interval {
		datapoints = append(datapoints, schema.Point{Val: math.NaN(), Ts: ts})
	}

	datapoints = append(datapoints, in.Datapoints...)

	canonicalTs := (datapoints[len(datapoints)-1].Ts / interval) * interval
	numDrop := int((datapoints[len(datapoints)-1].Ts - canonicalTs) / in.Interval)
	datapoints = datapoints[0 : len(datapoints)-numDrop]

	in.Datapoints = consolidation.Consolidate(datapoints, interval/in.Interval, in.Consolidator)
	in.Interval = interval
	dataMap.Add(Req{}, in)
	return in
}
