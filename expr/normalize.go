package expr

import (
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
// the following MUST be true when calling this:
// * interval > in.Interval
// * interval % in.Interval == 0
func normalizeTo(cache map[Req][]models.Series, in models.Series, interval uint32) models.Series {
	// we need to copy the datapoints first because the consolidater will reuse the input slice
	// TODO verify that Consolidate()'s behavior around odd-sized inputs makes sense. reread old stuff re canonical form etc
	datapoints := pointSlicePool.Get().([]schema.Point)
	datapoints = append(datapoints, in.Datapoints...)
	in.Datapoints = consolidation.Consolidate(datapoints, interval/in.Interval, in.Consolidator)
	in.Interval = interval / in.Interval
	cache[Req{}] = append(cache[Req{}], in)
	return in
}
