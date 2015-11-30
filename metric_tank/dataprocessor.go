package main

import (
	"errors"
	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/go-tsz"
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
	// TODO assert same length
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
	points := make([]Point, (len(in)/num)+1)
	for inpos, p := range in {
		bufpos = inpos % num
		buf[bufpos] = p.Val
		if bufpos == num-1 {
			points = append(points, Point{aggFunc(buf), p.Ts})
		}
	}
	if bufpos != -1 && bufpos < num-1 {
		// we have an incomplete buf of some points that didn't get aggregated yet
		points = append(points, Point{aggFunc(buf[:bufpos+1]), in[len(in)-1].Ts})
	}
	return points
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

func getTarget(key string, fromUnix, toUnix, minDataPoints, maxDataPoints uint32, consolidator consolidation.Consolidator, aggSettings []aggSetting) (points []Point, err error) {
	defer doRecover(&err)
	archive := -1 // -1 means original data, 0 last agg level, 1 2nd last, etc.

	p := make([]planOption, len(aggSettings)+1)
	guess := false

	// note: the metacache is clearly not a perfect all-knowning entity, it just knows the last interval of metrics seen since program start
	// and we assume we can use that interval through history.
	// TODO: no support for interval changes, metrics not seen yet, missing datablocks, ...
	interval := uint32(metaCache.Get(key))

	// we don't have the data yet, let's assume the interval is 10 seconds
	if interval == 0 {
		guess = true
		interval = 10
	}
	numPoints := (toUnix - fromUnix) / interval

	if guess {
		p[0] = planOption{"raw", 10, true, numPoints, ""}
	} else {
		p[0] = planOption{"raw", interval, false, numPoints, ""}
	}

	aggs := aggSettingsSpanDesc(aggSettings)
	sort.Sort(aggs)
	finished := false
	for i, aggSetting := range aggs {
		numPointsHere := (toUnix - fromUnix) / aggSetting.span
		p[i+1] = planOption{fmt.Sprintf("agg %d", i), aggSetting.span, false, numPointsHere, ""}
		if numPointsHere >= minDataPoints && !finished {
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
	runtimeConsolidation := (numPoints > maxDataPoints) // do we need to compress any points at runtime?

	fmt.Printf("%s %d-%d (%d) %d <= num <= %d. %s\n", key, fromUnix, toUnix, toUnix-fromUnix, minDataPoints, maxDataPoints, consolidator)
	fmt.Println("type   interval   points")
	sortedPlan := plan(p)
	sort.Sort(sortedPlan)
	for _, opt := range p {
		iStr := fmt.Sprintf("%d", opt.interval)
		if opt.intestim {
			iStr = fmt.Sprintf("%d (guess)", opt.interval)
		}
		fmt.Printf("%-6s %-10s %-6d %s\n", opt.archive, iStr, opt.points, opt.comment)
	}
	fmt.Printf("runtimeConsolidation: %t\n\n", runtimeConsolidation)

	if !readConsolidated && !runtimeConsolidation {
		return getSeries(key, consolidation.None, 0, fromUnix, toUnix), nil
	} else if !readConsolidated && runtimeConsolidation {
		return consolidate(
			getSeries(key, consolidation.None, 0, fromUnix, toUnix),
			int(numPoints/maxDataPoints),
			consolidator), nil
	} else if readConsolidated && !runtimeConsolidation {
		if consolidator == consolidation.Avg {
			return divide(
				getSeries(key, consolidation.Sum, interval, fromUnix, toUnix),
				getSeries(key, consolidation.Sum, interval, fromUnix, toUnix),
			), nil
		} else {
			return getSeries(key, consolidator, interval, fromUnix, toUnix), nil
		}
	} else {
		// readConsolidated && runtimeConsolidation
		aggNum := int(numPoints / maxDataPoints)
		if consolidator == consolidation.Avg {
			return divide(
				consolidate(
					getSeries(key, consolidation.Sum, interval, fromUnix, toUnix),
					aggNum,
					consolidation.Sum),
				consolidate(
					getSeries(key, consolidation.Cnt, interval, fromUnix, toUnix),
					aggNum,
					consolidation.Cnt),
			), nil
		} else {
			return consolidate(
				getSeries(key, consolidator, interval, fromUnix, toUnix),
				aggNum, consolidator), nil
		}
	}
}

// getSeries just gets the needed raw iters from mem and/or cassandra, based on from/to
// it can query for data within aggregated archives, by using fn min/max/sos/sum/cnt and providing the matching agg span.
func getSeries(key string, consolidator consolidation.Consolidator, aggSpan, fromUnix, toUnix uint32) []Point {
	iters := make([]*tsz.Iter, 0)
	var memIters []*tsz.Iter
	oldest := toUnix
	if metric, ok := metrics.Get(key); ok {
		if consolidator != consolidation.None {
			oldest, memIters = metric.GetAggregated(consolidator, aggSpan, fromUnix, toUnix)
		} else {
			oldest, memIters = metric.Get(fromUnix, toUnix)
		}
	} else {
		memIters = make([]*tsz.Iter, 0)
	}
	if oldest > fromUnix {
		reqSpanBoth.Value(int64(toUnix - fromUnix))
		log.Debug("data load from cassandra: %s - %s from mem: %s - %s", TS(fromUnix), TS(oldest), TS(oldest), TS(toUnix))
		if consolidator != consolidation.None {
			key = fmt.Sprintf("%s_%s_%s", key, consolidator.Archive(), aggSpan)
		}
		storeIters, err := searchCassandra(key, fromUnix, oldest)
		if err != nil {
			panic(err)
		}
		iters = append(iters, storeIters...)
	} else {
		reqSpanMem.Value(int64(toUnix - fromUnix))
		log.Debug("data load from mem: %s-%s, oldest (%d)", TS(fromUnix), TS(toUnix), oldest)
	}
	iters = append(iters, memIters...)

	points := make([]Point, 0)
	for _, iter := range iters {
		for iter.Next() {
			ts, val := iter.Values()
			if ts >= fromUnix && ts < toUnix {
				points = append(points, Point{val, ts})
			}
		}
	}
	return points
}
