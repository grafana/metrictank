package main

import (
	"fmt"
	"github.com/dgryski/go-tsz"
	"sort"
)

func getPoints(iters []*tsz.Iter, fromUnix, toUnix uint32) []Point {
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

func getDividedPoints(itersA, itersB []*tsz.Iter, fromUnix, toUnix uint32) []Point {
	// TODO maybe verify block corruption here. normally the iters should return the exact same amount of points, same timestamps, etc
	points := make([]Point, 0)
	for i, iterA := range itersA {
		iterB := itersB[i]
		for iterA.Next() && iterB.Next() {
			ts, a := iterA.Values()
			if ts >= fromUnix && ts < toUnix {
				_, b := iterB.Values()
				points = append(points, Point{a / b, ts})
			}
		}
	}
	return points
}

func getConsolidatedPoints(iters []*tsz.Iter, fromUnix, toUnix, aggNum uint32, consolidator aggregator) []Point {
	consFunc := getAggFunc(consolidator)
	buf := make([]float64, aggNum)
	i := 0
	lastTs := uint32(0)
	pos := 0
	points := make([]Point, 0)
	for _, iter := range iters {
		for iter.Next() {
			ts, val := iter.Values()
			if ts >= fromUnix && ts < toUnix {
				pos = i % int(aggNum)
				buf[pos] = val
				if pos == int(aggNum-1) {
					points = append(points, Point{consFunc(buf), ts})
				}
				lastTs = ts
				i++
			}
		}
	}
	if i > 0 && pos < int(aggNum-1) {
		// we have an incomplete buf of some points that didn't get aggregated yet
		points = append(points, Point{consFunc(buf[:pos+1]), lastTs})
	}
	return points
}

func getConsolidatedDividedPoints(itersA, itersB []*tsz.Iter, fromUnix, toUnix uint32, consolidatorA, consolidatorB aggregator, aggNum int) []Point {
	consFuncA := getAggFunc(consolidatorA)
	consFuncB := getAggFunc(consolidatorB)
	bufA := make([]float64, aggNum)
	bufB := make([]float64, aggNum)
	i := 0
	lastTs := uint32(0)
	pos := 0
	points := make([]Point, 0)
	for i, iterA := range itersA {
		iterB := itersB[i]
		for iterA.Next() && iterB.Next() {
			ts, a := iterA.Values()
			if ts >= fromUnix && ts < toUnix {
				_, b := iterB.Values()
				pos = i % int(aggNum)
				bufA[pos] = a
				bufB[pos] = b
				if pos == aggNum-1 {
					points = append(points, Point{consFuncA(bufA) / consFuncB(bufB), ts})
				}
				lastTs = ts
				i++
			}
		}
	}
	if i > 0 && pos < aggNum-1 {
		// we have an incomplete buf of some points that didn't get aggregated yet
		points = append(points, Point{consFuncA(bufA[:pos+1]) / consFuncB(bufB[:pos+1]), lastTs})
	}
	return points
}

func getTarget(key string, fromUnix, toUnix, minDataPoints, maxDataPoints uint32, consolidator aggregator, aggSettings []aggSetting) ([]Point, error) {
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

	consolidate := (numPoints > maxDataPoints) // do we need to compress any points at runtime?
	readConsolidated := (archive != -1)        // do we need to read from a downsampled series?

	if !consolidate && !readConsolidated {
		// no consolidation at all needed
		iters, err := getSeries(key, "", 0, fromUnix, toUnix)
		if err != nil {
			return nil, err
		}
		points := getPoints(iters, fromUnix, toUnix)
	}
	if consolidate && !readConsolidated {
		// only runtime consolidation is needed
		// every buf can be processed by a func
		iters, err := getSeries(key, "", 0, fromUnix, toUnix)
		if err != nil {
			return nil, err
		}
		aggNum := numPoints / maxDataPoints
		points := getConsolidatedPoints(iters, fromUnix, toUnix, aggNum, consolidator)
	}
	if !consolidate && readConsolidated {
		// we have to read from a consolidated archive but don't have to apply runtime consolidation
		// just read straight from archives. for avg, do the math
		points := make([]Point, 0)
		if consolidator == avg {
			sumiters, err := getSeries(key, "sum", interval, fromUnix, toUnix)
			if err != nil {
				return nil, err
			}
			cntiters, err := getSeries(key, "cnt", interval, fromUnix, toUnix)
			if err != nil {
				return nil, err
			}
			points = getDividedPoints(sumiters, cntiters, fromUnix, toUnix)
		} else {
			iters, err := getSeries(key, consolidator.String(), interval, fromUnix, toUnix)
			if err != nil {
				return nil, err
			}
			points := getPoints(iters, fromUnix, toUnix)
		}
	}
	if consolidate && readConsolidated {
		// we'll read from a consolidated archive and need to apply further consolidation on top
		points := make([]Point, 0)
		if consolidator == avg {
			sumiters, err := getSeries(key, "sum", interval, fromUnix, toUnix)
			if err != nil {
				return nil, err
			}
			cntiters, err := getSeries(key, "cnt", interval, fromUnix, toUnix)
			if err != nil {
				return nil, err
			}
			points = getConsolidatedDividedPoints(sumiters, cntiters, fromUnix, toUnix)
		} else {
			consFunc := getAggFunc(consolidator)
			iters, err := getSeries(key, consFunc, interval, fromUnix, toUnix)
			if err != nil {
				return nil, err
			}
			aggNum := numPoints / maxDataPoints
			points := getConsolidatedPoints(iters, fromUnix, toUnix, aggNum, consolidator)
		}
	}
}

// getSeries just gets the needed raw iters from mem and/or cassandra, based on from/to
// it can query for data within aggregated archives, by using fn min/max/sos/sum/cnt and providing the matching agg span.
func getSeries(key, consolidator aggregator, aggSpan, fromUnix, toUnix uint32) ([]*tsz.Iter, error) {
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
			return nil, err
		}
		iters = append(iters, storeIters...)
	} else {
		reqSpanMem.Value(int64(toUnix - fromUnix))
		log.Debug("data load from mem: %s-%s, oldest (%d)", TS(fromUnix), TS(toUnix), oldest)
	}
	iters = append(iters, memIters...)
	return iters, nil
}
