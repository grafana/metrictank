package expr

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/util"
)

// normalize normalizes series to the same common LCM interval - if they don't already have the same interval
// any adjusted series gets created in a series drawn out of the pool and is added to the cache so it can be reclaimed
func normalize(cache map[Req][]models.Series, in []models.Series) []models.Series {
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
			in[i] = normalizeTo(cache, s, lcm)
		}
	}
	return in
}

func normalizeTwo(cache map[Req][]models.Series, a, b models.Series) (models.Series, models.Series) {
	if a.Interval == b.Interval {
		return a, b
	}
	intervals := []uint32{a.Interval, b.Interval}
	lcm := util.Lcm(intervals)

	if a.Interval != lcm {
		a = normalizeTo(cache, a, lcm)
	}
	if b.Interval != lcm {
		b = normalizeTo(cache, b, lcm)
	}
	return a, b
}

// normalizeTo normalizes the given series to the desired interval
// also, any points that would lead to a non-canonical result are removed.
// the following MUST be true when calling this:
// * interval > in.Interval
// * interval % in.Interval == 0
func normalizeTo(cache map[Req][]models.Series, in models.Series, interval uint32) models.Series {
	// we need to copy the datapoints first because the consolidater will reuse the input slice
	datapoints := pointSlicePool.Get().([]schema.Point)

	// scroll to the first point for which ts-in.Interval % interval = 0
	// (e.g. if in.Interval 10, interval 30, and series [60,70,80,90,...] we want to start at 70, such that (70,80,90) forms the first point with ts=90

	i := 0
	var found bool
	for i <= len(in.Datapoints) {
		if in.Datapoints[i].Ts-in.Interval%interval == 0 {
			found = true
			break
		}
		i++
	}
	if !found {
		panic(fmt.Sprintf("series %q cannot be normalized from interval %d to %d because it is too short. please request a longer timeframe", in.Target, in.Interval, interval))
	}

	// scroll back to the last point for which ts-in.Interval % interval = 0, which would be the first point to exclude.
	// (e.g. if in.Interval 10, interval 30, and series [...,110,120,130,140] we want to exclude 130 and end at 120, such that (100,110,120) forms the last point with ts=120.

	j := len(in.Datapoints) - 1
	found = false
	for j >= 0 {
		if in.Datapoints[j].Ts-in.Interval%interval == 0 {
			found = true
			break
		}
		j--
	}
	if !found || j == i {
		panic(fmt.Sprintf("series %q cannot be normalized from interval %d to %d because it is too short. please request a longer timeframe", in.Target, in.Interval, interval))
	}

	datapoints = append(datapoints, in.Datapoints[i:j]...)
	in.Datapoints = consolidation.Consolidate(datapoints, interval/in.Interval, in.Consolidator)
	in.Interval = interval / in.Interval
	cache[Req{}] = append(cache[Req{}], in)
	return in
}
