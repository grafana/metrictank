package main

import (
	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"sort"
)

type Req struct {
	// these fields can be set straight away:
	key          string
	from         uint32
	to           uint32
	minPoints    uint32
	maxPoints    uint32
	consolidator consolidation.Consolidator

	// these fields need some more coordination and are typically set later
	archive     int    // -1 means original data, 0 last agg level, 1 2nd last, etc.
	rawInterval uint32 // the interval of the metric
	interval    uint32 // the interval we want for the fetch
	numPoints   uint32 // numPoints for the fetch
}

func NewReq(key string, from, to, minPoints, maxPoints uint32, consolidator consolidation.Consolidator) Req {
	return Req{
		key,
		from,
		to,
		minPoints,
		maxPoints,
		consolidator,
		-2, // this is supposed to be updated still!
		0,  // this is supposed to be updated still
		0,  // this is supposed to be updated still
		0,  // this is supposed to be updated still
	}
}

func (r Req) String() string {
	return fmt.Sprintf("%s %d - %d (%s - %s) span:%ds. %d <= points <= %d. %s", r.key, r.from, r.to, TS(r.from), TS(r.to), r.to-r.from-1, r.minPoints, r.maxPoints, r.consolidator)
}

// sets archive, numPoints, interval
func (r *Req) optimize(startInterval uint32, aggSettings []aggSetting) {
	// typically the specified startInterval will be our rawInterval, but if we're part of a set of requests for other metrics,
	// then startInterval may not be our rawInterval.  It may or may not be one of our native aggregation interval though

	siInAgg := false // does the startInterval match one of the aggregations?
	siArchive := -2  // which archive shall we use to satisfy the start interval?
	for i, aggSetting := range aggSettings {
		if aggSetting.span == startInterval {
			siInAgg = true
			siArchive = i
		}
	}

	if startInterval == r.rawInterval {
		siArchive = -1
	}
	// if startInterval isn't corresponding to any of those archives, we need to find out what would be the best way to achieve it
	// trough runtime consolidation, i.e. found the archive with the lowest multiple.
	// a future optimization could be take into account how many points we have in RAM,
	// maybe it could be better to use data in RAM even when it's higher res, than fetching from cassandra
	if siArchive == -2 {
		for i, aggSetting := range aggSettings {
			if aggSetting.span > startInterval && aggSetting.span%startInterval == 0 {
				siArchive = i
				break
			}
		}
	}
	// we're going to assume we can always find at least a multiple.  If that doesn't work and the program ever crashes here, you should
	// rethink your intervals and aggregation settings.
	if siArchive == -2 {
		panic(fmt.Sprintf("best common interval is %d which cannot be satisfied by raw our aggregated series. come on.", startInterval))
	}

	// we'll have 1 option for raw,
	// an option for each aggregation,
	// and possibly an extra one if startInterval doesn't match any of those.
	var p []planOption
	if startInterval != r.rawInterval && !siInAgg {
		p = make([]planOption, len(aggSettings)+2)
	} else {
		p = make([]planOption, len(aggSettings)+1)
	}

	p[0] = planOption{"raw", -1, r.rawInterval, r.numPoints, ""}
	for i, aggSetting := range aggSettings {
		numPointsHere := (r.to - r.from) / aggSetting.span
		p[i] = planOption{fmt.Sprintf("agg %d", i), i, aggSetting.span, numPointsHere, ""}
	}
	if startInterval != r.rawInterval && !siInAgg {
		p[len(p)-1] = planOption{"intermediate", siArchive, startInterval, (r.to - r.from) / startInterval, ""}
	}
	sp := plan(p)
	sort.Sort(sp)

	// params to get started with. will be updated if we have better suited data
	r.archive = -1
	r.numPoints = (r.to - r.from) / r.rawInterval
	r.interval = r.rawInterval

	var chosen int
	for i, option := range p {
		if option.points >= r.minPoints {
			r.archive = option.archive
			r.interval = option.interval
			r.numPoints = option.points
		}
		chosen = i
	}
	p[chosen].comment = "<-- chosen"

	// note, it should always be safe to dynamically switch on/off consolidation based on how well our data stacks up against the request
	// i.e. whether your data got consolidated or not, it should be pretty equivalent.
	// for that reason, stdev should not be done as a consolidation. but sos is still useful for when we explicitly (and always, not optionally) want the stdev.

	// note: we print all physical data bands (raw+ consolidated),
	// but also the intermediate optimisation in case of multi requests, if it has a unique interval, which is nothing more
	// than a runtime consolidated band which we otherwise normally don't show as a band.  that could be slightly confusing but oh well.

	for _, opt := range sp {
		log.Debug("%-6s %-6d %-6d %s", opt.title, opt.interval, opt.points, opt.comment)
	}
}
