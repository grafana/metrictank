package api

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/iter"
	"github.com/raintank/metrictank/util"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

const defaultPointSliceSize = 2000

var pointSlicePool = sync.Pool{
	// default size is probably bigger than what most responses need, but it saves [re]allocations
	// also it's possible that occasionnally more size is needed, causing a realloc of underlying array, and that extra space will stick around until next GC run.
	New: func() interface{} { return make([]schema.Point, 0, defaultPointSliceSize) },
}

// fix assures all points are nicely aligned (quantized) and padded with nulls in case there's gaps in data
// graphite does this quantization before storing, we may want to do that as well at some point
// note: values are quantized to the right because we can't lie about the future:
// e.g. if interval is 10 and we have a point at 8 or at 2, it will be quantized to 10, we should never move
// values to earlier in time.
func fix(in []schema.Point, from, to, interval uint32) []schema.Point {
	// first point should have the first timestamp >= from that divides by interval
	first := from
	remain := from % interval
	if remain != 0 {
		first = from + interval - remain
	}

	// last point should have the last timestamp < to that divides by interval (because to is always exclusive)
	last := (to - 1) - ((to - 1) % interval)

	if last < first {
		// the requested range is too narrow for the requested interval
		return []schema.Point{}
	}
	out := make([]schema.Point, (last-first)/interval+1)

	// i iterates in. o iterates out. t is the ts we're looking to fill.
	for t, i, o := first, 0, -1; t <= last; t += interval {
		o += 1

		// input is out of values. add a null
		if i >= len(in) {
			out[o] = schema.Point{Val: math.NaN(), Ts: t}
			continue
		}

		p := in[i]
		if p.Ts == t {
			// point has perfect ts, use it and move on to next point
			out[o] = p
			i++
		} else if p.Ts > t {
			// point is too recent, append a null and reconsider same point for next slot
			out[o] = schema.Point{Val: math.NaN(), Ts: t}
		} else if p.Ts > t-interval && p.Ts < t {
			// point is a bit older, so it's good enough, just quantize the ts, and move on to next point for next round
			out[o] = schema.Point{Val: p.Val, Ts: t}
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
			o -= 1
		}
	}

	return out
}

func divide(pointsA, pointsB []schema.Point) []schema.Point {
	if len(pointsA) != len(pointsB) {
		panic(fmt.Errorf("divide of a series with len %d by a series with len %d", len(pointsA), len(pointsB)))
	}
	for i := range pointsA {
		pointsA[i].Val /= pointsB[i].Val
	}
	return pointsA
}

func consolidate(in []schema.Point, aggNum uint32, consolidator consolidation.Consolidator) []schema.Point {
	num := int(aggNum)
	aggFunc := consolidation.GetAggFunc(consolidator)
	outLen := len(in) / num
	var out []schema.Point
	cleanLen := num * outLen // what the len of input slice would be if it was a perfect fit
	if len(in) == cleanLen {
		out = in[0:outLen]
		out_i := 0
		var next_i int
		for in_i := 0; in_i < cleanLen; in_i = next_i {
			next_i = in_i + num
			out[out_i] = schema.Point{Val: aggFunc(in[in_i:next_i]), Ts: in[next_i-1].Ts}
			out_i += 1
		}
	} else {
		outLen += 1
		out = in[0:outLen]
		out_i := 0
		var next_i int
		for in_i := 0; in_i < cleanLen; in_i = next_i {
			next_i = in_i + num
			out[out_i] = schema.Point{Val: aggFunc(in[in_i:next_i]), Ts: in[next_i-1].Ts}
			out_i += 1
		}
		// we have some leftover points that didn't get aggregated yet
		// we must also aggregate it and add it, and the timestamp of this point must be what it would have been
		// if the group would have been complete, i.e. points in the consolidation output should be evenly spaced.
		// obviously we can only figure out the interval if we have at least 2 points
		var lastTs uint32
		if len(in) == 1 {
			lastTs = in[0].Ts
		} else {
			interval := in[len(in)-1].Ts - in[len(in)-2].Ts
			// len 10, cleanLen 9, num 3 -> 3*4 values supposedly -> "in[11].Ts" -> in[9].Ts + 2*interval
			lastTs = in[cleanLen].Ts + (aggNum-1)*interval
		}
		out[out_i] = schema.Point{Val: aggFunc(in[cleanLen:]), Ts: lastTs}
	}
	return out
}

// returns how many points should be aggregated together so that you end up with as many points as possible,
// but never more than maxPoints
func aggEvery(numPoints, maxPoints uint32) uint32 {
	if numPoints == 0 {
		return 1
	}
	return (numPoints + maxPoints - 1) / maxPoints
}

// error is the error of the first failing target request
func (s *Server) getTargets(reqs []models.Req) ([]models.Series, error) {
	seriesChan := make(chan models.Series, len(reqs))
	errorsChan := make(chan error, len(reqs))

	// split reqs into local and remote.
	localReqs := make([]models.Req, 0)
	remoteReqs := make(map[*cluster.Node][]models.Req)
	for _, req := range reqs {
		if req.Node == nil || req.Node == cluster.ThisNode {
			localReqs = append(localReqs, req)
		} else {
			remoteReqs[req.Node] = append(remoteReqs[req.Node], req)
		}
	}

	for node, nodeReqs := range remoteReqs {
		buf, err := node.Post("/internal/getdata", models.GetData{Requests: nodeReqs})
		if err != nil {
			return nil, err
		}
		var resp models.GetDataResp
		buf, err = resp.UnmarshalMsg(buf)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("HTTP error unmarshaling body from %s/internal/getdata: %q", node.GetName(), err))
		}
		if len(buf) != 0 {
			return nil, errors.New(fmt.Sprintf("%s/internal/getdata: returned extra data", node.GetName()))
		}
		return resp.Series, nil
	}

	// TODO: abort pending requests on error, maybe use context, maybe timeouts too
	wg := sync.WaitGroup{}
	wg.Add(len(localReqs))
	for _, req := range localReqs {
		go func(wg *sync.WaitGroup, req models.Req) {
			pre := time.Now()
			points, interval, err := s.getTarget(req)
			if err != nil {
				errorsChan <- err
			} else {
				getTargetDuration.Value(time.Now().Sub(pre))
				seriesChan <- models.Series{
					Target:     req.Target,
					Datapoints: points,
					Interval:   interval,
				}
			}
			wg.Done()
		}(&wg, req)
	}
	go func() {
		wg.Wait()
		close(seriesChan)
		close(errorsChan)
	}()
	out := make([]models.Series, 0, len(reqs))
	var err error
	for series := range seriesChan {
		out = append(out, series)
	}
	for e := range errorsChan {
		err = e
		break
	}
	return out, err

}

func (s *Server) getTarget(req models.Req) (points []schema.Point, interval uint32, err error) {
	readConsolidated := req.Archive != 0   // do we need to read from a downsampled series?
	runtimeConsolidation := req.AggNum > 1 // do we need to compress any points at runtime?

	if LogLevel < 2 {
		if runtimeConsolidation {
			log.Debug("DP getTarget() %s runtimeConsolidation: true. agg factor: %d -> output interval: %d", req, req.AggNum, req.OutInterval)
		} else {
			log.Debug("DP getTarget() %s runtimeConsolidation: false. output interval: %d", req, req.OutInterval)
		}
	}

	if !readConsolidated && !runtimeConsolidation {
		return fix(
			s.getSeries(req.Key, consolidation.None, 0, req.From, req.To),
			req.From,
			req.To,
			req.ArchInterval,
		), req.OutInterval, nil
	} else if !readConsolidated && runtimeConsolidation {
		return consolidate(
			fix(
				s.getSeries(req.Key, consolidation.None, 0, req.From, req.To),
				req.From,
				req.To,
				req.ArchInterval,
			),
			req.AggNum,
			req.Consolidator), req.OutInterval, nil
	} else if readConsolidated && !runtimeConsolidation {
		if req.Consolidator == consolidation.Avg {
			return divide(
				fix(
					s.getSeries(req.Key, consolidation.Sum, req.ArchInterval, req.From, req.To),
					req.From,
					req.To,
					req.ArchInterval,
				),
				fix(
					s.getSeries(req.Key, consolidation.Cnt, req.ArchInterval, req.From, req.To),
					req.From,
					req.To,
					req.ArchInterval,
				),
			), req.OutInterval, nil
		} else {
			return fix(
				s.getSeries(req.Key, req.Consolidator, req.ArchInterval, req.From, req.To),
				req.From,
				req.To,
				req.ArchInterval,
			), req.OutInterval, nil
		}
	} else {
		// readConsolidated && runtimeConsolidation
		if req.Consolidator == consolidation.Avg {
			return divide(
				consolidate(
					fix(
						s.getSeries(req.Key, consolidation.Sum, req.ArchInterval, req.From, req.To),
						req.From,
						req.To,
						req.ArchInterval,
					),
					req.AggNum,
					consolidation.Sum),
				consolidate(
					fix(
						s.getSeries(req.Key, consolidation.Cnt, req.ArchInterval, req.From, req.To),
						req.From,
						req.To,
						req.ArchInterval,
					),
					req.AggNum,
					consolidation.Sum),
			), req.OutInterval, nil
		} else {
			return consolidate(
				fix(
					s.getSeries(req.Key, req.Consolidator, req.ArchInterval, req.From, req.To),
					req.From,
					req.To,
					req.ArchInterval,
				),
				req.AggNum, req.Consolidator), req.OutInterval, nil
		}
	}
}

func logLoad(typ, key string, from, to uint32) {
	if LogLevel < 2 {
		log.Debug("DP load from %-6s %-20s %d - %d (%s - %s) span:%ds", typ, key, from, to, util.TS(from), util.TS(to), to-from-1)
	}
}

func aggMetricKey(key, archive string, aggSpan uint32) string {
	return fmt.Sprintf("%s_%s_%d", key, archive, aggSpan)
}

// getSeries just gets the needed raw iters from mem and/or cassandra, based on from/to
// it can query for data within aggregated archives, by using fn min/max/sum/cnt and providing the matching agg span.
func (s *Server) getSeries(key string, consolidator consolidation.Consolidator, aggSpan, fromUnix, toUnix uint32) []schema.Point {
	iters := make([]iter.Iter, 0)
	memIters := make([]iter.Iter, 0)
	oldest := toUnix
	if metric, ok := s.MemoryStore.Get(key); ok {
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
		until := util.Min(oldest, toUnix)
		logLoad("cassan", key, fromUnix, until)
		storeIters, err := s.BackendStore.Search(key, fromUnix, until)
		if err != nil {
			panic(err)
		}
		iters = append(iters, storeIters...)
	} else {
		reqSpanMem.Value(int64(toUnix - fromUnix))
	}
	pre := time.Now()
	iters = append(iters, memIters...)

	points := pointSlicePool.Get().([]schema.Point)
	for _, iter := range iters {
		total := 0
		good := 0
		for iter.Next() {
			total += 1
			ts, val := iter.Values()
			if ts >= fromUnix && ts < toUnix {
				good += 1
				points = append(points, schema.Point{Val: val, Ts: ts})
			}
		}
		if LogLevel < 2 {
			if iter.Cass {
				log.Debug("DP getSeries: iter cass %d values good/total %d/%d", iter.T0, good, total)
			} else {
				log.Debug("DP getSeries: iter mem %d values good/total %d/%d", iter.T0, good, total)
			}
		}
	}
	itersToPointsDuration.Value(time.Now().Sub(pre))
	return points
}

// check for duplicate series names. If found merge the results.
func mergeSeries(in []models.Series) []models.Series {
	seriesByTarget := make(map[string][]models.Series)
	for _, series := range in {
		seriesByTarget[series.Target] = append(seriesByTarget[series.Target], series)
	}
	merged := make([]models.Series, len(seriesByTarget))
	i := 0
	for _, series := range seriesByTarget {
		if len(series) == 1 {
			merged[i] = series[0]
		} else {
			//we use the first series in the list as our result.  We check over every
			// point and if it is null, we then check the other series for a non null
			// value to use instead.
			log.Debug("%s has multiple series.", series[0].Target)
			for i, _ := range series[0].Datapoints {
				for j := 0; j < len(series); j++ {
					if !math.IsNaN(series[j].Datapoints[i].Val) {
						series[0].Datapoints[i].Val = series[j].Datapoints[i].Val
						break
					}
				}
			}
			merged[i] = series[0]
		}
		i++
	}
	return merged
}
