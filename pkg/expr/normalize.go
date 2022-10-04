package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/api/seriescycle"
	"github.com/grafana/metrictank/pkg/consolidation"
	"github.com/grafana/metrictank/pkg/schema"
	"github.com/grafana/metrictank/pkg/util"
	"github.com/grafana/metrictank/pkg/util/align"
)

// Normalize normalizes series to the same common LCM interval - if they don't already have the same interval
// any adjusted series gets created in a series drawn out of the pool
func Normalize(in []models.Series, sc seriescycle.SeriesCycler) []models.Series {
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
			in[i] = NormalizeTo(s, lcm, sc)
		}
	}
	return in
}

func NormalizeTwo(a, b models.Series, sc seriescycle.SeriesCycler) (models.Series, models.Series) {
	if a.Interval == b.Interval {
		return a, b
	}
	intervals := []uint32{a.Interval, b.Interval}
	lcm := util.Lcm(intervals)

	if a.Interval != lcm {
		a = NormalizeTo(a, lcm, sc)
	}
	if b.Interval != lcm {
		b = NormalizeTo(b, lcm, sc)
	}
	return a, b
}

// NormalizeTo normalizes the given series to the desired interval
// will pad front and strip from back as needed, to assure the output is canonical for the given interval
// the following MUST be true when calling this:
// * interval > in.Interval
// * interval % in.Interval == 0
// the adjusted series gets created in a series drawn out of the pool
func NormalizeTo(in models.Series, interval uint32, sc seriescycle.SeriesCycler) models.Series {

	if len(in.Datapoints) == 0 {
		panic(fmt.Sprintf("series %q cannot be normalized from interval %d to %d because it is empty", in.Target, in.Interval, interval))
	}

	// we need to copy the datapoints first because the consolidater will reuse the input slice
	// also, for the consolidator's output to be canonical, the input must be pre-canonical.
	// so add nulls in front and at the back to make it pre-canonical.
	// this may make points in front and at the back less accurate when consolidated (e.g. summing when some of the points are null results in a lower value)
	// but this is what graphite does....
	datapoints := pointSlicePool.GetMin(len(in.Datapoints))
	datapoints = makePreCanonicalCopy(in, interval, datapoints)

	// series may have been created by a function that didn't know which consolidation function to default to.
	// in the future maybe we can do more clever things here. e.g. perSecond maybe consolidate by max.
	if in.Consolidator == 0 {
		in.Consolidator = consolidation.Avg
	}

	sc.Done(in)
	in.Datapoints = consolidation.Consolidate(datapoints, 0, interval/in.Interval, in.Consolidator)
	in.Interval = interval
	sc.New(in)

	return in
}

// makePreCanonicalCopy returns a copy of in's datapoints slice, but adjusted to be pre-canonical with respect to interval.
// for this, it reuses the 'datapoints' slice.
func makePreCanonicalCopy(in models.Series, interval uint32, datapoints []schema.Point) []schema.Point {
	// to achieve this we need to assure our input starts and ends with the right timestamp.

	// we need to figure out what is the ts of the first point to feed into the consolidator
	// example of how this works:
	// if in.Interval is 5, and interval is 15, then for example, to generate point 15, because we postmark and we want a full input going into this point,
	// you want inputs 5, 10 and 15.
	// or more generally (you can follow any example vertically):
	//  5 10 15 20 25 30 35 40 45 50 <-- if any of these timestamps are your first point in `in`
	//  5  5  5 20 20 20 35 35 35 50 <-- then these are the corresponding timestamps of the first values we want as input for the consolidator
	// 15 15 15 30 30 30 45 45 45 60 <-- which, when fed through alignForwardIfNotAligned(), result in these numbers
	//  5  5  5 20 20 20 35 35 35 50 <-- subtract (aggnum-1)* in.interval or equivalent -interval + in.Interval = -15 + 5 = -10. this is our initial timestamp.

	canonicalStart := align.ForwardIfNotAligned(in.Datapoints[0].Ts, interval) - interval + in.Interval
	for ts := canonicalStart; ts < in.Datapoints[0].Ts; ts += in.Interval {
		datapoints = append(datapoints, schema.Point{Val: math.NaN(), Ts: ts})
	}

	datapoints = append(datapoints, in.Datapoints...)

	// for the desired last input ts, it's important to be aware of cases like this:
	// until=47, interval=10, in.Interval = 5
	// a canonical 10s series would have as last point 40. whereas our input series will have 45, which will consolidate into a point with timestamp 50, which is incorrect
	// (it breaches `to`, and may have more points than other series it needs to be combined with)
	// thus, we also need to potentially trim points from the back until the last point has the same Ts as a canonical series would

	canonicalTs := (datapoints[len(datapoints)-1].Ts / interval) * interval
	numDrop := int((datapoints[len(datapoints)-1].Ts - canonicalTs) / in.Interval)
	return datapoints[0 : len(datapoints)-numDrop]
}
