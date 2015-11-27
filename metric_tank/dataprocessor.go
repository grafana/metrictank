package main

import (
	"fmt"
	"github.com/dgryski/go-tsz"
	"sort"
)

// doRecover is the handler that turns panics into returns from the top level of getTarget.
func doRecover(errp *error) {
	e := recover()
	if e != nil {
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		*errp = e.(error)
	}
	return
}

func divide(pointsA, pointsB []Point) []Point {
	// TODO assert same length
	out := make([]Point, len(pointsA))
	for i, a := range pointsA {
		b := pointsB[i]
		out[i] = Point{a / b, ts}
	}
	return out
}

func consolidate(in []Point, num int, consolidator aggregator) []Point {
	consFunc := getAggFunc(consolidator)
	buf := make([]float64, num)
	lastTs := uint32(0)
	bufpos := 0
	points := make([]Point, (len(in)/num)+1)
	for inpos, val := range in {
		bufpos = inpos % num
		buf[bufpos] = val
		if bufpos == num-1 {
			points = append(points, Point{consFunc(buf), ts})
		}
		lastTs = ts
	}
	if inpos > 0 && bufpos < num-1 {
		// we have an incomplete buf of some points that didn't get aggregated yet
		points = append(points, Point{consFunc(buf[:bufpos+1]), lastTs})
	}
	return points
}

func getTarget(key string, fromUnix, toUnix, minDataPoints, maxDataPoints uint32, consolidator aggregator, aggSettings []aggSetting) (points []Point, err error) {
	defer doRecover(&err)
	archive := -1 // -1 means original data, 0 last agg level, 1 2nd last, etc.

	// note: the metacache is clearly not a perfect all-knowning entity, it just knows the last interval of metrics seen since program start
	// and we assume we can use that interval through history.
	// TODO: no support for interval changes, metrics not seen yet, missing datablocks, ...
	interval := uint32(metaCache.Get(key))
	numPoints := (toUnix - fromUnix) / interval

	aggs := aggSettingsSpanDesc(aggSettings)
	sort.Sort(aggs)
	for i, aggSetting := range aggs {
		fmt.Println("key", key, aggSetting.span)
		numPointsHere := (toUnix - fromUnix) / aggSetting.span
		if numPointsHere >= minDataPoints {
			archive = i
			interval = aggSetting.chunkSpan
			numPoints = numPointsHere
			break
		}
	}

	// archives can be: min max sos sum cnt
	// consolidator   : avg last min max sum
	// mapping:
	// consolidator  -> archives
	// ------------------------
	// avg           -> sum/cnt
	// last          -> ? // not implemented yet
	// min           -> min
	// max           -> max
	// sum           -> sum

	// note, it should always be safe to dynamically switch on/off consolidation based on how well our data stacks up against the request
	// i.e. whether your data got consolidated or not, it should be pretty equivalent.
	// for that reason, stdev should not be done as a consolidation. but sos is still useful for when we explicitly (and always, not optionally) want the stdev.

	readConsolidated := (archive != -1)                 // do we need to read from a downsampled series?
	runtimeConsolidation := (numPoints > maxDataPoints) // do we need to compress any points at runtime?

	if !readConsolidated && !runtimeConsolidation {
		return getSeries(key, "", 0, fromUnix, toUnix), nil
	}
	if !readConsolidated && runtimeConsolidation {
		return consolidate(
			getSeries(key, "", 0, fromUnix, toUnix),
			int(numPoints/maxDataPoints),
			consolidator), nil
	}
	if readConsolidated && !runtimeConsolidation {
		points := make([]Point, 0)
		if consolidator == avg {
			return divide(
				getSeries(key, "sum", interval, fromUnix, toUnix),
				getSeries(key, "cnt", interval, fromUnix, toUnix),
			), nil
		} else {
			return getSeries(key, consolidator.String(), interval, fromUnix, toUnix), nil
		}
	}
	if readConsolidated && runtimeConsolidation {
		aggNum := int(numPoints / maxDataPoints)
		if consolidator == avg {
			return divide(
				consolidate(
					getSeries(key, "sum", interval, fromUnix, toUnix),
					aggNum,
					"sum"),
				consolidate(
					getSeries(key, "cnt", interval, fromUnix, toUnix),
					aggNum,
					"cnt"),
			), nil
		} else {
			consFunc := getAggFunc(consolidator)
			return consolidate(
				getSeries(key, consFunc, interval, fromUnix, toUnix),
				aggNum, consolidator), nil
		}
	}
}

// getSeries just gets the needed raw iters from mem and/or cassandra, based on from/to
// it can query for data within aggregated archives, by using fn min/max/sos/sum/cnt and providing the matching agg span.
func getSeries(key, consolidator aggregator, aggSpan, fromUnix, toUnix uint32) []Point {
	iters := make([]*tsz.Iter, 0)
	var memIters []*tsz.Iter
	oldest := toUnix
	if metric, ok := metrics.Get(key); ok {
		if aggfn != none {
			oldest, memIters = metric.GetAggregated(aggregator, aggSpan, fromUnix, toUnix)
		} else {
			oldest, memIters = metric.Get(fromUnix, toUnix)
		}
	} else {
		memIters = make([]*tsz.Iter, 0)
	}
	if oldest > fromUnix {
		reqSpanBoth.Value(int64(toUnix - fromUnix))
		log.Debug("data load from cassandra: %s - %s from mem: %s - %s", TS(fromUnix), TS(oldest), TS(oldest), TS(toUnix))
		if aggfn != "" {
			key = fmt.Sprintf("%s_%s_%s", key, aggregator.String(), span)
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
