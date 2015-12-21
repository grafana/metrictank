package main

import (
	"errors"
	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"math"
	"runtime"
	"sort"
)

// doRecover is the handler that turns panics into returns from the top level of getTarget.
func doRecover(errp *error) {
	e := recover()
	if e != nil {
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		if err, ok := e.(error); ok {
			*errp = err
		} else if errStr, ok := e.(string); ok {
			*errp = errors.New(errStr)
		} else {
			*errp = fmt.Errorf("%v", e)
		}
	}
	return
}

// fix assures all points are nicely aligned (quantized) and padded with nulls in case there's gaps in data
// graphite does this quantization before storing, we may want to do that as well at some point
// note: values are quantized to the right because we can't lie about the future:
// e.g. if interval is 10 and we have a point at 8 or at 2, it will be quantized to 10, we should never move
// values to earlier in time.
func fix(in []Point, from, to, interval uint32) []Point {
	out := make([]Point, 0, len(in))

	// first point should be the first point at or after from that divides by interval
	start := from
	remain := from % interval
	if remain != 0 {
		start = from + interval - remain
	}

	// last point should be the last value that divides by interval lower than to (because to is always exclusive)
	lastPoint := (to - 1) - ((to - 1) % interval)

	for t, i := start, 0; t <= lastPoint; t += interval {

		// input is out of values. add a null
		if i >= len(in) {
			out = append(out, Point{math.NaN(), t})
			continue
		}

		p := in[i]
		if p.Ts == t {
			// point has perfect ts, use it and move on to next point
			out = append(out, p)
			i++
		} else if p.Ts > t {
			// point is too recent, append a null and reconsider same point for next slot
			out = append(out, Point{math.NaN(), t})
		} else if p.Ts > t-interval && p.Ts < t {
			// point is a bit older, so it's good enough, just quantize the ts, and move on to next point for next round
			out = append(out, Point{p.Val, t})
			i++
		} else if p.Ts <= t-interval {
			// point is too old. advance until we find a point that is recent enough, and then go through the considerations again,
			// if those considerations are any of the above ones.
			// if the last point would end up in this branch again, discard it as well.
			for p.Ts <= t-interval && i < len(in)-1 {
				i++
				p = in[i]
			}
			if p.Ts <= t-interval {
				i++
			}
			t -= interval
		}

	}

	return out
}

func divide(pointsA, pointsB []Point) []Point {
	if len(pointsA) != len(pointsB) {
		panic(fmt.Errorf("divide of a series with len %d by a series with len %d", len(pointsA), len(pointsB)))
	}
	out := make([]Point, len(pointsA))
	for i, a := range pointsA {
		b := pointsB[i]
		out[i] = Point{a.Val / b.Val, a.Ts}
	}
	return out
}

func consolidate(in []Point, aggNum uint32, consolidator consolidation.Consolidator) []Point {
	num := int(aggNum)
	aggFunc := consolidation.GetAggFunc(consolidator)
	buf := make([]float64, num)
	bufpos := -1
	outLen := len(in) / num
	if len(in)%num != 0 {
		outLen += 1
	}
	points := make([]Point, 0, outLen)
	for inpos, p := range in {
		bufpos = inpos % num
		buf[bufpos] = p.Val
		if bufpos == num-1 {
			points = append(points, Point{aggFunc(buf), p.Ts})
		}
	}
	if bufpos != -1 && bufpos < num-1 {
		// we have an incomplete buf of some points that didn't get aggregated yet
		// we must also aggregate it and add it, and the timestamp of this point must be what it would have been
		// if the buf would have been complete, i.e. points in the consolidation output should be evenly spaced.
		// obviously we can only figure out the interval if we have at least 2 points
		var lastTs uint32
		if len(in) == 1 {
			lastTs = in[0].Ts
		} else {
			interval := in[len(in)-1].Ts - in[len(in)-2].Ts
			// len 10, num 3 -> 3*4 values supposedly -> "in[11].Ts" -> in[9].Ts + 2*interval
			lastTs = in[len(in)-1].Ts + uint32(num-len(in)%num)*interval
		}
		points = append(points, Point{aggFunc(buf[:bufpos+1]), lastTs})
	}
	return points
}

// returns how many points should be aggregated together so that you end up with as many points as possible,
// but never more than maxPoints
func aggEvery(numPoints, maxPoints uint32) int {
	return int((numPoints + maxPoints - 1) / maxPoints)
}

type band struct {
	title    string
	archive  int
	interval uint32
	ramSpan  uint32 // what's the span we have in memory
	comment  string
	cost     uint32 // computed when solving for best requests
}

func (b band) String() string {
	return fmt.Sprintf("<band %d: %s> int:%d, ram:%d, cost: %d, comment: %s", b.archive, b.title, b.interval, b.ramSpan, b.cost, b.comment)
}

// returns cost, aggNum and whether possible at all
// by comparing the band with the requested output interval of the req.
func (b *band) costFor(r Req) (uint32, uint32, bool) {
	b.cost = 0
	timespan := r.to - r.from

	// settings assuming no runtime consolidation
	finalPoints := timespan / b.interval
	aggNum := uint32(1)

	// this band can't be used at all.
	if b.interval > r.outInterval {
		fmt.Println("cost ", b, r.DebugString(), "IMP too >int")
		return 0, 0, false
	} else if b.interval < r.outInterval {
		// this band can't be aggregated to match the requested interval
		if r.outInterval%b.interval != 0 {
			fmt.Println("cost ", b, r.DebugString(), "IMPOS, int%")
			return 0, 0, false
		}
		aggNum := r.outInterval / b.interval
		// add the cost for each point that should be runtime-consolidated away
		finalPoints = finalPoints / aggNum
		b.cost += ((aggNum - 1) * finalPoints) * 10
	}
	if finalPoints > r.maxPoints || finalPoints < r.minPoints {
		b.cost = 0
		fmt.Println("cost ", b, r.DebugString(), "IMP #points")
		return 0, 0, false
	}

	// add cost for data that should be fetched from storage
	// TODO this doesn't take into account from-to, it assumes to=now.
	if b.ramSpan < timespan {
		b.cost += (timespan - b.ramSpan) / b.interval * 500
	}
	// add cost for just having a lot of points in output
	b.cost += finalPoints
	fmt.Println("cost ", b, r.DebugString())

	return b.cost, aggNum, true
}

type bands []band

func (a bands) Len() int           { return len(a) }
func (a bands) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bands) Less(i, j int) bool { return a[i].interval < a[j].interval }

type solution struct {
	reqs  []Req
	bands []int // for each req, point to the best band
	cost  uint32
}

func NewSolution() *solution {
	return &solution{
		make([]Req, 0),
		make([]int, 0),
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
	fmt.Println("ALIGN START")

	// model all the bands for each requested metric
	// the 0th band is always the raw series, with highest res (lowest interval)
	aggs := aggSettingsSpanAsc(aggSettings)
	sort.Sort(aggs)
	allbands := make([][]band, len(reqs))
	for i, req := range reqs {
		allbands[i] = make([]band, len(aggs)+1)

		// model the first band, the raw storage
		allbands[i][0] = band{"raw ", -1, req.rawInterval, ramSpan, "", 0}

		// now model the bands we get from the aggregations
		for j, agg := range aggs {
			ramSpan := agg.chunkSpan * (agg.numChunks - 1)
			allbands[i][j+1] = band{fmt.Sprintf("agg %d", j), j, agg.span, ramSpan, "", 0}
		}
	}

	// now we know for each metric all the available bands.
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
	for _, bands := range allbands {
		if bands[0].interval > highestLowestInterval {
			highestLowestInterval = bands[0].interval
		}
	}

	// now let's find the best interval based on what all the metrics can return.
	// solutions are scored based on 3 factors, in order of importance:
	// 1) the more we can fulfill by using data in RAM as opposed to external storage, the better
	// 2) least amount of runtime consolidation possible.
	// 3) least amount of points needed, but still satisfying minDataPoints

	// note that for some metrics, highestLowestInterval may not occur in any of the bands natively. not raw, not in the aggregations.
	// so the possible output intervals are all sensible intervals between
	// highestLowestInterval (which either matches, or is larger than the raw band) and
	// the maximum interval possible given minDataPoints.
	low := highestLowestInterval
	interval := (reqs[0].to - reqs[0].from)
	high := interval / reqs[0].minPoints
	if interval%reqs[0].minPoints != 0 {
		high += 1
	}
	outputIntervals := make([]uint32, 0)
	for i := low; i <= high; i += 10 {
		outputIntervals = append(outputIntervals, i)
	}

	// there's a solution for each input interval (hopefully)
	// each solution is a set of requests to satisfy all targets for said interval, along with associated cost to satisfy all those requests
	solutions := make([]solution, 0, len(outputIntervals))
INTERVALS:
	for _, interval := range outputIntervals {
		s := NewSolution()
		fmt.Println("working on solution for interval", interval)
		for j, req := range reqs {
			// for this given req and interval, find the band with the lowest cost.
			req.outInterval = interval
			lowestCost := uint32(math.MaxUint32)
			bestBand := -1
			bestAggNum := uint32(0)
			for k, band := range allbands[j] {
				cost, aggNum, possible := band.costFor(req)
				if possible && cost < lowestCost {
					lowestCost = cost
					bestBand = k
					bestAggNum = aggNum
				}
			}
			// this metric has no bands to satisfy this interval at all
			// so this interval has no solution
			if bestBand == -1 {
				continue INTERVALS
			} else {
				band := allbands[j][bestBand]
				s.cost += band.cost
				req.archive = band.archive
				req.archInterval = band.interval
				req.aggNum = bestAggNum
				s.reqs = append(s.reqs, req)
				s.bands = append(s.bands, bestBand)
			}
		}
		// we're still here, so we've built a complete solution that can satisfy each request for a given total cost.
		solutions = append(solutions, *s)
	}
	if len(solutions) == 0 {
		return nil, errors.New("minDataPoints/maxDataPoints cannot be honored with the series requested")
	}
	lowestCost := uint32(math.MaxUint32)
	bestSolution := -1
	for i, solution := range solutions {
		if solution.cost < lowestCost {
			fmt.Println("yeay solution", i, "only has cost", solution.cost)
			lowestCost = solution.cost
			bestSolution = i
		}
	}
	solution := solutions[bestSolution]

	for i, req := range solution.reqs {
		allbands[i][solution.bands[i]].comment = "<-- chosen"

		// note, it should always be safe to dynamically switch on/off consolidation based on how well our data stacks up against the request
		// i.e. whether your data got consolidated or not, it should be pretty equivalent.
		// for that reason, stdev should not be done as a consolidation. but sos is still useful for when we explicitly (and always, not optionally) want the stdev.

		for _, band := range allbands[i] {
			log.Debug("%-6s %-6d %-6d %s", band.title, band.interval, (req.to-req.from)/band.interval, band.comment)
		}
	}
	return solution.reqs, nil

}

func getTarget(req Req) (points []Point, interval uint32, err error) {
	defer doRecover(&err)

	readConsolidated := req.archive != -1  // do we need to read from a downsampled series?
	runtimeConsolidation := req.aggNum > 1 // do we need to compress any points at runtime?

	log.Debug("getTarget()         %s", req)
	log.Debug("type   interval   points")

	if runtimeConsolidation {
		log.Debug("runtimeConsolidation: true. agg factor: %d -> output interval: %d", req.aggNum, req.outInterval)
	} else {
		log.Debug("runtimeConsolidation: false. output interval: %d", req.outInterval)
	}

	if !readConsolidated && !runtimeConsolidation {
		return fix(
			getSeries(req.key, consolidation.None, 0, req.from, req.to),
			req.from,
			req.to,
			req.archInterval,
		), req.outInterval, nil
	} else if !readConsolidated && runtimeConsolidation {
		return consolidate(
			fix(
				getSeries(req.key, consolidation.None, 0, req.from, req.to),
				req.from,
				req.to,
				req.archInterval,
			),
			req.aggNum,
			req.consolidator), req.outInterval, nil
	} else if readConsolidated && !runtimeConsolidation {
		if req.consolidator == consolidation.Avg {
			return divide(
				fix(
					getSeries(req.key, consolidation.Sum, req.archInterval, req.from, req.to),
					req.from,
					req.to,
					req.archInterval,
				),
				fix(
					getSeries(req.key, consolidation.Cnt, req.archInterval, req.from, req.to),
					req.from,
					req.to,
					req.archInterval,
				),
			), req.outInterval, nil
		} else {
			return fix(
				getSeries(req.key, req.consolidator, req.archInterval, req.from, req.to),
				req.from,
				req.to,
				req.archInterval,
			), req.outInterval, nil
		}
	} else {
		// readConsolidated && runtimeConsolidation
		if req.consolidator == consolidation.Avg {
			return divide(
				consolidate(
					fix(
						getSeries(req.key, consolidation.Sum, req.archInterval, req.from, req.to),
						req.from,
						req.to,
						req.archInterval,
					),
					req.aggNum,
					consolidation.Sum),
				consolidate(
					fix(
						getSeries(req.key, consolidation.Cnt, req.archInterval, req.from, req.to),
						req.from,
						req.to,
						req.archInterval,
					),
					req.aggNum,
					consolidation.Cnt),
			), req.outInterval, nil
		} else {
			return consolidate(
				fix(
					getSeries(req.key, req.consolidator, req.archInterval, req.from, req.to),
					req.from,
					req.to,
					req.archInterval,
				),
				req.aggNum, req.consolidator), req.outInterval, nil
		}
	}
}

func logLoad(typ, key string, from, to uint32) {
	log.Debug("load from %-6s %-20s %d - %d (%s - %s) span:%ds", typ, key, from, to, TS(from), TS(to), to-from-1)
}

func aggMetricKey(key, archive string, aggSpan uint32) string {
	return fmt.Sprintf("%s_%s_%d", key, archive, aggSpan)
}

// getSeries just gets the needed raw iters from mem and/or cassandra, based on from/to
// it can query for data within aggregated archives, by using fn min/max/sos/sum/cnt and providing the matching agg span.
func getSeries(key string, consolidator consolidation.Consolidator, aggSpan, fromUnix, toUnix uint32) []Point {
	iters := make([]Iter, 0)
	memIters := make([]Iter, 0)
	oldest := toUnix
	if metric, ok := metrics.Get(key); ok {
		if consolidator != consolidation.None {
			logLoad("memory", aggMetricKey(key, consolidator.Archive(), aggSpan), fromUnix, toUnix)
			oldest, memIters = metric.GetAggregated(consolidator, aggSpan, fromUnix, toUnix)
		} else {
			logLoad("memory", key, fromUnix, toUnix)
			oldest, memIters = metric.Get(fromUnix, toUnix)
		}
	}
	if oldest > fromUnix {
		reqSpanBoth.Value(int64(toUnix - fromUnix))
		if consolidator != consolidation.None {
			key = aggMetricKey(key, consolidator.Archive(), aggSpan)
		}
		// if oldest < to -> search until oldest, we already have the rest from mem
		// if to < oldest -> no need to search until oldest, only search until to
		until := min(oldest, toUnix)
		logLoad("cassan", key, fromUnix, until)
		storeIters, err := searchCassandra(key, fromUnix, until)
		if err != nil {
			panic(err)
		}
		iters = append(iters, storeIters...)
	} else {
		reqSpanMem.Value(int64(toUnix - fromUnix))
	}
	iters = append(iters, memIters...)

	points := make([]Point, 0)
	for _, iter := range iters {
		total := 0
		good := 0
		for iter.Next() {
			total += 1
			ts, val := iter.Values()
			if ts >= fromUnix && ts < toUnix {
				good += 1
				points = append(points, Point{val, ts})
			}
		}
		log.Debug("getSeries: iter %s  values good/total %d/%d", iter.cmt, good, total)
	}
	return points
}
