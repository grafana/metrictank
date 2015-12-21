package main

import (
	"errors"
	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"math"
	"sort"
)

// represents a data "archive", i.e. the raw one, or an aggregated series
type archive struct {
	title    string
	key      int // 0 for raw, 1 for first aggregation, etc.
	interval uint32
	ramSpan  uint32 // what's the span we have in memory
	comment  string
	cost     uint32 // computed when solving for best requests
}

func (b archive) String() string {
	return fmt.Sprintf("<archive %d: %s> int:%d, ram:%d, cost: %d, comment: %s", b.key, b.title, b.interval, b.ramSpan, b.cost, b.comment)
}

// returns cost, aggNum and whether possible at all
// by comparing the archive with the requested output interval of the req.
func (b *archive) costFor(r Req) (uint32, uint32, bool) {
	b.cost = 0
	timespan := r.to - r.from

	// settings assuming no runtime consolidation
	finalPoints := timespan / b.interval
	aggNum := uint32(1)

	if b.interval > r.outInterval {
		// this archive can't be used at all.
		return 0, 0, false
	} else if b.interval < r.outInterval {
		if r.outInterval%b.interval != 0 {
			// this archive can't be aggregated to match the requested interval
			return 0, 0, false
		}
		aggNum = r.outInterval / b.interval
		// add the cost for each point that should be runtime-consolidated away
		finalPoints = finalPoints / aggNum
		b.cost += ((aggNum - 1) * finalPoints) * 10
	}
	if finalPoints > r.maxPoints || finalPoints < r.minPoints {
		b.cost = 0
		return 0, 0, false
	}

	// add cost for data that should be fetched from storage
	// TODO this doesn't take into account from-to, it assumes to=now.
	if b.ramSpan < timespan {
		b.cost += (timespan - b.ramSpan) / b.interval * 500
	}
	// add cost for just having a lot of points in output
	b.cost += finalPoints

	return b.cost, aggNum, true
}

type archives []archive

func (a archives) Len() int           { return len(a) }
func (a archives) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a archives) Less(i, j int) bool { return a[i].interval < a[j].interval }

// provides exact execution details for each request, as well as a cost of executing everything
type solution struct {
	reqs []Req
	cost uint32
}

func NewSolution() *solution {
	return &solution{
		make([]Req, 0),
		0,
	}
}

func findMetricsForRequests(reqs []Req, metaCache *MetaCache) error {
	for i := range reqs {
		err := metaCache.UpdateReq(&reqs[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// updates the requests with all details for fetching, making sure all metrics are in the same, optimal interval
// luckily, all metrics still use the same aggSettings, making this a bit simpler
// for all requests, sets archive, numPoints, interval (and rawInterval as a side effect)
// note: it is assumed that all requests have the same from, to, minDataPoints and maxdatapoints!
func alignRequests(reqs []Req, ramSpan uint32, aggSettings []aggSetting) ([]Req, error) {

	// model all the archives for each requested metric
	// the 0th archive is always the raw series, with highest res (lowest interval)
	aggs := aggSettingsSpanAsc(aggSettings)
	sort.Sort(aggs)
	allarchives := make([][]archive, len(reqs))
	for i, req := range reqs {
		allarchives[i] = make([]archive, len(aggs)+1)

		// model the first archive, the raw storage
		allarchives[i][0] = archive{"raw ", 0, req.rawInterval, ramSpan, "", 0}

		// now model the archives we get from the aggregations
		for j, agg := range aggs {
			ramSpan := agg.chunkSpan * (agg.numChunks - 1)
			allarchives[i][j+1] = archive{fmt.Sprintf("agg %d", j), j + 1, agg.span, ramSpan, "", 0}
		}
	}

	// now we know for each metric all the available archives.
	// first find the highest raw interval amongst the metrics
	// this is effectively the lowest interval that can be returned by all of the requested metrics
	// some may need to do runtime consolidation for that to happen though
	// eg for
	// 10 raw, 600, 7200, 21600
	// 30 raw, 600, 7200, 21600
	// 60 raw, 600, 7200, 21600
	// would return 60
	// an optimization to avoid intervals that for many of the metrics would have to be runtime consolidated
	// can be added later.
	highestLowestInterval := uint32(0)
	for _, archives := range allarchives {
		if archives[0].interval > highestLowestInterval {
			highestLowestInterval = archives[0].interval
		}
	}

	// now let's find the best interval based on what all the metrics can return.
	// solutions are scored based on 3 factors, in order of importance:
	// 1) the more we can fulfill by using data in RAM as opposed to external storage, the better
	// 2) least amount of runtime consolidation possible.
	// 3) least amount of points needed, but still satisfying minDataPoints

	// note that for some metrics, highestLowestInterval may not occur in any of the archives natively. not raw, not in the aggregations.
	// so the possible output intervals are all sensible intervals between
	// highestLowestInterval (which either matches, or is larger than the raw archive) and
	// the maximum interval possible given minDataPoints.
	low := highestLowestInterval
	interval := (reqs[0].to - reqs[0].from)
	high := interval / reqs[0].minPoints
	if interval%reqs[0].minPoints != 0 {
		high += 1
	}
	outputIntervals := make([]uint32, 0)
	for i := low; i < high+10; i += 10 {
		outputIntervals = append(outputIntervals, i)
	}

	// there's a solution for each input interval (hopefully)
	// each solution is a set of requests to satisfy all targets for said interval, along with associated cost to satisfy all those requests
	solutions := make([]*solution, 0, len(outputIntervals))
INTERVALS:
	for _, interval := range outputIntervals {
		s := NewSolution()
		for j, req := range reqs {
			// for this given req and interval, find the archive with the lowest cost.
			req.outInterval = interval
			lowestCost := uint32(math.MaxUint32)
			for _, archive := range allarchives[j] {
				cost, aggNum, possible := archive.costFor(req)
				if possible && cost < lowestCost {
					lowestCost = cost
					req.archive = archive.key
					req.archInterval = archive.interval
					req.aggNum = aggNum
				}
			}
			// this metric has no archives to satisfy this interval at all
			// so this interval has no solution
			if lowestCost == uint32(math.MaxUint32) {
				continue INTERVALS
			} else {
				s.cost += lowestCost
				s.reqs = append(s.reqs, req)
			}
		}
		// we're still here, so we've built a complete solution that can satisfy each request for a given total cost.
		solutions = append(solutions, s)
	}
	if len(solutions) == 0 {
		return nil, errors.New("minDataPoints/maxDataPoints cannot be honored with the series requested")
	}
	lowestCost := uint32(math.MaxUint32)
	bestSolution := -1
	for i, solution := range solutions {
		if solution.cost < lowestCost {
			lowestCost = solution.cost
			bestSolution = i
		}
	}
	solution := solutions[bestSolution]

	for i, req := range solution.reqs {
		allarchives[i][solution.reqs[i].archive].comment = "<-- chosen"

		// note, it should always be safe to dynamically switch on/off consolidation based on how well our data stacks up against the request
		// i.e. whether your data got consolidated or not, it should be pretty equivalent.
		// for that reason, stdev should not be done as a consolidation. but sos is still useful for when we explicitly (and always, not optionally) want the stdev.

		for _, archive := range allarchives[i] {
			log.Debug("%-6s %-6d %-6d %s", archive.title, archive.interval, (req.to-req.from)/archive.interval, archive.comment)
		}
	}
	return solution.reqs, nil

}
