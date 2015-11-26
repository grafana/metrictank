package main

import (
	"github.com/dgryski/go-tsz"
)

func getTarget(key string, fromUnix, toUnix uint32, minDataPoints, maxDataPoints int, consolidator aggregator) ([]Point, error) {
	archive := -1 // -1 means original data, 0 last agg level, 1 2nd last, etc.

	// note: the metacache is clearly not a perfect all-knowning entity, it just knows the last interval of metrics seen since program start
	// and we assume we can use that interval through history.
	// TODO: no support for interval changes, metrics not seen yet, missing datablocks, ...
	interval := metaCache.Get(key)
	numPoints = int(toUnix-fromUnix) / interval

	aggs := aggSettingsSpanDesc(aggSettings)
	sort.Sort(aggs)
	for i, aggSetting := range aggs {
		fmt.Println("key", key, aggSetting.span)
		numPointsHere = int((toUnix - fromUnix) / aggSetting.span)
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
	readConsolidated := (archive != -1) // do we need to read from a downsampled series?

	if !consolidate && !readConsolidated {
		// no consolidation at all needed
		points := make([]Point, 0)
		 iters, err := getSeries(key, "", -1, fromUnix, toUnix)
		if err != nil {
			return nil, err
		}
		for _, iter := range iters {
			for iter.Next() {
				ts, val := iter.Values()
				if ts >= fromUnix && ts < toUnix {
					points = append(points, Point{val, ts})
				}
			}
		}
	}
	if consolidate && !readConsolidated {
		// only runtime consolidation is needed
	}
	if !consolidate && readConsolidated {
		// we have to read from a consolidated archive but have to apply to runtime consolidation
	}
	if consolidate && readConsolidated {
		// we'll read from a consolidated archive and need to apply further consolidation on top
	}

	} else {
		// if we're reading from an archived series and consolidateBy is avg, we should read sum and cnt streams and divide them
		 sumiters, err := getSeries(key, "", -1, fromUnix, toUnix)
		if err != nil {
			return nil, err
		}
			// get sum, get cnt, check same length
		}
	} else {
		sum /cnt 
		iters, err :=getSeries(key, aggfn string, interval, fromUnix, toUnix uint32)
		aggNum := numPoints / maxDataPoints
		buf := make([]float64, aggNum)
		i := 0
		lastTs := uint32(0)
		pos := 0
		for _, iter := range iters {
			for iter.Next() {
				ts, val := iter.Values()
				if ts >= fromUnix && ts < toUnix {
					pos = i % aggNum
					buf[pos] = val
					if pos == aggNum-1 {
						points = append(points, Point{consFunc(buf), ts})
					}
					lastTs = ts
					i++
				}
			}
		}
		if i > 0 && pos < aggNum-1 {
			// we have an incomplete buf of some points that didn't get aggregated yet
			points = append(points, Point{consFunc(buf[:pos+1]), lastTs})
		}
	}
}

// getSeries just gets the needed raw iters from mem and/or cassandra, based on from/to
// it can query for data within aggregated archives, by using fn min/max/sos/sum/cnt and providing the matching agg span.
func getSeries(key, aggfn string, aggSpan, fromUnix, toUnix uint32) ([]*tsz.Iter, error) {
	iters := make([]*tsz.Iter, 0)
	var memIters []*tsz.Iter
	oldest := toUnix
	if metric, ok := metrics.Get(key); ok {
		if aggfn != "" {
			oldest, memIters = metric.GetAggregated(aggfn, aggSpan, fromUnix, toUnix)
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
			key = fmt.Sprintf("%s_%s_%s", key, aggfn, span)
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
