package main

import (
	"errors"
	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
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

func consolidate(in []Point, num int, consolidator consolidation.Consolidator) []Point {
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

type planOption struct {
	archive  string
	interval uint32
	intestim bool
	points   uint32
	comment  string
}

type plan []planOption

func (a plan) Len() int           { return len(a) }
func (a plan) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a plan) Less(i, j int) bool { return a[i].points > a[j].points }

func getTarget(req Req, aggSettings []aggSetting, metaCache *MetaCache) (points []Point, err error) {
	defer doRecover(&err)
	archive := -1 // -1 means original data, 0 last agg level, 1 2nd last, etc.

	p := make([]planOption, len(aggSettings)+1)
	guess := false

	// note: the metacache is clearly not a perfect all-knowning entity, it just knows the last interval of metrics seen since program start
	// and we assume we can use that interval through history.
	// TODO: no support for interval changes, metrics not seen yet, missing datablocks, ...
	meta := metaCache.Get(req.key)
	interval := uint32(meta.interval)

	// we don't have the data yet, let's assume the interval is 10 seconds
	if interval == 0 {
		guess = true
		interval = 10
	}
	numPoints := (req.to - req.from) / interval

	p[0] = planOption{"raw", interval, guess, numPoints, ""}

	aggs := aggSettingsSpanDesc(aggSettings)
	sort.Sort(aggs)
	finished := false
	for i, aggSetting := range aggs {
		numPointsHere := (req.to - req.from) / aggSetting.span
		p[i+1] = planOption{fmt.Sprintf("agg %d", i), aggSetting.span, false, numPointsHere, ""}
		if numPointsHere >= req.minPoints && !finished {
			archive = i
			interval = aggSetting.span
			numPoints = numPointsHere
			finished = true
		}
	}

	p[archive+1].comment = "<-- chosen"

	// note, it should always be safe to dynamically switch on/off consolidation based on how well our data stacks up against the request
	// i.e. whether your data got consolidated or not, it should be pretty equivalent.
	// for that reason, stdev should not be done as a consolidation. but sos is still useful for when we explicitly (and always, not optionally) want the stdev.

	readConsolidated := (archive != -1)                 // do we need to read from a downsampled series?
	runtimeConsolidation := (numPoints > req.maxPoints) // do we need to compress any points at runtime?

	log.Debug("getTarget()         %s", req)
	log.Debug("type   interval   points")
	sortedPlan := plan(p)
	sort.Sort(sortedPlan)
	for _, opt := range p {
		iStr := fmt.Sprintf("%d", opt.interval)
		if opt.intestim {
			iStr = fmt.Sprintf("%d (guess)", opt.interval)
		}
		log.Debug("%-6s %-10s %-6d %s", opt.archive, iStr, opt.points, opt.comment)
	}
	log.Debug("runtimeConsolidation: %t", runtimeConsolidation)

	if !readConsolidated && !runtimeConsolidation {
		return getSeries(req.key, consolidation.None, 0, req.from, req.to), nil
	} else if !readConsolidated && runtimeConsolidation {
		return consolidate(
			getSeries(req.key, consolidation.None, 0, req.from, req.to),
			aggEvery(numPoints, req.maxPoints),
			req.consolidator), nil
	} else if readConsolidated && !runtimeConsolidation {
		if req.consolidator == consolidation.Avg {
			return divide(
				getSeries(req.key, consolidation.Sum, interval, req.from, req.to),
				getSeries(req.key, consolidation.Cnt, interval, req.from, req.to),
			), nil
		} else {
			return getSeries(req.key, req.consolidator, interval, req.from, req.to), nil
		}
	} else {
		// readConsolidated && runtimeConsolidation
		aggNum := aggEvery(numPoints, req.maxPoints)
		if req.consolidator == consolidation.Avg {
			return divide(
				consolidate(
					getSeries(req.key, consolidation.Sum, interval, req.from, req.to),
					aggNum,
					consolidation.Sum),
				consolidate(
					getSeries(req.key, consolidation.Cnt, interval, req.from, req.to),
					aggNum,
					consolidation.Cnt),
			), nil
		} else {
			return consolidate(
				getSeries(req.key, req.consolidator, interval, req.from, req.to),
				aggNum, req.consolidator), nil
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
