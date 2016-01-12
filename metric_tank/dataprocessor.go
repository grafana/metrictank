package main

import (
	"errors"
	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"math"
	"runtime"
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
func aggEvery(numPoints, maxPoints uint32) uint32 {
	return (numPoints + maxPoints - 1) / maxPoints
}

func getTarget(req Req) (points []Point, interval uint32, err error) {
	defer doRecover(&err)

	readConsolidated := req.archive != 0   // do we need to read from a downsampled series?
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
					consolidation.Sum),
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
