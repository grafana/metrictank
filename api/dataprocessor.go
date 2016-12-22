package api

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/mdata/chunk"
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

func (s *Server) getTargets(reqs []models.Req) ([]models.Series, error) {
	// split reqs into local and remote.
	localReqs := make([]models.Req, 0)
	remoteReqs := make(map[*cluster.Node][]models.Req)
	for _, req := range reqs {
		if req.Node == cluster.ThisNode {
			localReqs = append(localReqs, req)
		} else {
			remoteReqs[req.Node] = append(remoteReqs[req.Node], req)
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	out := make([]models.Series, 0)
	errs := make([]error, 0)

	if len(localReqs) > 0 {
		wg.Add(1)
		go func() {
			// the only errors returned are from us catching panics, so we should treat them
			// all as internalServerErrors
			series, err := s.getTargetsLocal(localReqs)
			mu.Lock()
			if err != nil {
				errs = append(errs, err)
			}
			if len(series) > 0 {
				out = append(out, series...)
			}
			mu.Unlock()
			wg.Done()
		}()
	}
	if len(remoteReqs) > 0 {
		wg.Add(1)
		go func() {
			// all errors returned returned are *response.Error.
			series, err := s.getTargetsRemote(remoteReqs)
			mu.Lock()
			if err != nil {
				errs = append(errs, err)
			}
			if len(series) > 0 {
				out = append(out, series...)
			}
			mu.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	var err error
	if len(errs) > 0 {
		err = errs[0]
	}
	log.Debug("DP getTargets: %d series found on cluster", len(out))
	return out, err
}

func (s *Server) getTargetsRemote(remoteReqs map[*cluster.Node][]models.Req) ([]models.Series, error) {
	seriesChan := make(chan []models.Series, len(remoteReqs))
	errorsChan := make(chan error, len(remoteReqs))
	wg := sync.WaitGroup{}
	wg.Add(len(remoteReqs))
	for node, nodeReqs := range remoteReqs {
		log.Debug("DP getTargetsRemote: handling %d reqs from %s", len(nodeReqs), node.GetName())
		go func(reqs []models.Req, node *cluster.Node) {
			defer wg.Done()
			buf, err := node.Post("/getdata", models.GetData{Requests: reqs})
			if err != nil {
				errorsChan <- err
				return
			}
			var resp models.GetDataResp
			buf, err = resp.UnmarshalMsg(buf)
			if err != nil {
				log.Error(3, "DP getTargetsRemote: error unmarshaling body from %s/getdata: %q", node.GetName(), err)
				errorsChan <- err
				return
			}
			log.Debug("DP getTargetsRemote: %s returned %d series", node.GetName(), len(resp.Series))
			seriesChan <- resp.Series
		}(nodeReqs, node)
	}
	go func() {
		wg.Wait()
		close(seriesChan)
		close(errorsChan)
	}()
	out := make([]models.Series, 0)
	var err error
	for series := range seriesChan {
		out = append(out, series...)
	}
	log.Debug("DP getTargetsRemote: total of %d series found on peers", len(out))
	for e := range errorsChan {
		err = e
		break
	}
	return out, err
}

// error is the error of the first failing target request
func (s *Server) getTargetsLocal(reqs []models.Req) ([]models.Series, error) {
	log.Debug("DP getTargetsLocal: handling %d reqs locally", len(reqs))
	seriesChan := make(chan models.Series, len(reqs))
	errorsChan := make(chan error, len(reqs))
	// TODO: abort pending requests on error, maybe use context, maybe timeouts too
	wg := sync.WaitGroup{}
	wg.Add(len(reqs))
	for _, req := range reqs {
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
	log.Debug("DP getTargetsLocal: %d series found locally", len(out))
	for e := range errorsChan {
		err = e
		break
	}
	return out, err

}

func (s *Server) getTarget(req models.Req) (points []schema.Point, interval uint32, err error) {
	defer doRecover(&err)
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
		return s.getSeries(req, consolidation.None), req.OutInterval, nil
	} else if !readConsolidated && runtimeConsolidation {
		return consolidate(s.getSeries(req, consolidation.None), req.AggNum, req.Consolidator), req.OutInterval, nil
	} else if readConsolidated && !runtimeConsolidation {
		if req.Consolidator == consolidation.Avg {
			return divide(
				s.getSeries(req, consolidation.Sum),
				s.getSeries(req, consolidation.Cnt),
			), req.OutInterval, nil
		} else {
			return s.getSeries(req, req.Consolidator), req.OutInterval, nil
		}
	} else {
		// readConsolidated && runtimeConsolidation
		if req.Consolidator == consolidation.Avg {
			return divide(
				consolidate(s.getSeries(req, consolidation.Sum), req.AggNum, consolidation.Sum),
				consolidate(s.getSeries(req, consolidation.Cnt), req.AggNum, consolidation.Sum),
			), req.OutInterval, nil
		} else {
			return consolidate(
				s.getSeries(req, req.Consolidator), req.AggNum, req.Consolidator), req.OutInterval, nil
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

func prevBoundary(ts uint32, span uint32) uint32 {
	return ts - ((ts-1)%span + 1)
}

// getSeries returns points from mem (and cassandra if needed), within the range from (inclusive) - to (exclusive)
// it can query for data within aggregated archives, by using fn min/max/sum/cnt and providing the matching agg span as interval
// pass consolidation.None as consolidator to mean read from raw interval, otherwise we'll read from aggregated series.
// all data will also be quantized.
func (s *Server) getSeries(req models.Req, consolidator consolidation.Consolidator) []schema.Point {
	iters := make([]chunk.Iter, 0)
	memIters := make([]chunk.Iter, 0)
	oldest := req.To
	toUnix := req.To
	fromUnix := req.From
	interval := req.ArchInterval
	key := req.Key
	// while aggregated archives are quantized, raw intervals are not.  quantizing happens at the end of this function, **after* this step.
	// So we have to adjust the range to get the right data.
	// (ranges described as a..b include both and b)
	// REQ           0[---FROM---60]----------120-----------180[----TO----240]  any request from 1..60 to 181..240 should ...
	// QUANTD RESULT 0----------[60]---------[120]---------[180]                return points 60, 120 and 180 (simply because of to/from and inclusive/exclusive rules) ..
	// STORED DATA   0[----------60][---------120][---------180][---------240]  but data for 60 may be at 1..60, data for 120 at 61..120 and for 180 at 121..180 (due to quantizing)
	// to retrieve the stored data, we also use from inclusive and to exclusive,
	// so to make sure that the data after quantization (fix()) is correct, we have to make the following adjustment:
	// `from`   1..60 needs data    1..60   -> always adjust `from` to previous boundary+1 (here 1)
	// `to`  181..240 needs data 121..180   -> always adjust `to`   to previous boundary+1 (here 181)

	if consolidator == consolidation.None {
		fromUnix = prevBoundary(req.From, interval) + 1
		toUnix = prevBoundary(req.To, interval) + 1
	}

	if metric, ok := s.MemoryStore.Get(key); ok {
		if consolidator != consolidation.None {
			logLoad("memory", aggMetricKey(key, consolidator.Archive(), interval), fromUnix, toUnix)
			oldest, memIters = metric.GetAggregated(consolidator, interval, fromUnix, toUnix)
		} else {
			logLoad("memory", key, fromUnix, toUnix)
			oldest, memIters = metric.Get(fromUnix, toUnix)
		}
	}
	if oldest > fromUnix {
		reqSpanBoth.Value(int64(toUnix - fromUnix))
		if consolidator != consolidation.None {
			key = aggMetricKey(key, consolidator.Archive(), interval)
		}
		// if oldest < to -> search until oldest, we already have the rest from mem
		// if to < oldest -> no need to search until oldest, only search until to
		until := util.Min(oldest, toUnix)
		logLoad("cassan", key, fromUnix, until)

		log.Debug("cache: searching query key %s, from %d, until %d", key, fromUnix, until)
		cacheRes := s.Cache.Search(key, fromUnix, until)
		log.Debug("cache: result start %d, end %d", len(cacheRes.Start), len(cacheRes.End))
		// the request cannot completely be served from cache, it will require cassandra involvement
		if !cacheRes.Complete {
			storeIterGens, err := s.BackendStore.Search(key, cacheRes.From, cacheRes.Until)
			if err != nil {
				panic(err)
			}

			var prevts uint32 = 0
			for _, itgen := range cacheRes.Start {
				prevts = itgen.Ts()
				it, err := itgen.Get()
				if err != nil {
					// TODO(replay) figure out what to do if one piece is corrupt
					log.Error(3, "itergen: error getting iter from cache result start slice %+v", err)
					continue
				}
				iters = append(iters, *it)
			}
			for _, itgen := range storeIterGens {
				// it's important that the itgens get added in chronological order,
				// currently we rely on cassandra returning results in order
				go s.Cache.Add(key, prevts, itgen)
				prevts = itgen.Ts()
				it, err := itgen.Get()
				if err != nil {
					// TODO(replay) figure out what to do if one piece is corrupt
					log.Error(3, "itergen: error getting iter from cassandra slice %+v", err)
					continue
				}
				iters = append(iters, *it)
			}
			for i := len(cacheRes.End) - 1; i >= 0; i-- {
				it, err := cacheRes.End[i].Get()
				if err != nil {
					// TODO(replay) figure out what to do if one piece is corrupt
					log.Error(3, "itergen: error getting iter from cache result end slice %+v", err)
					continue
				}
				iters = append(iters, *it)
			}
		} else {
			for _, itgen := range cacheRes.Start {
				iter, err := itgen.Get()
				if err != nil {
					// TODO(replay) figure out what to do if one piece is corrupt
					log.Error(3, "itergen: error getting iter from Start list %+v", err)
					continue
				}
				iters = append(iters, *iter)
			}
		}
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
			log.Debug("DP getSeries: iter %d values good/total %d/%d", iter.T0, good, total)
		}
	}
	itersToPointsDuration.Value(time.Now().Sub(pre))
	return fix(points, fromUnix, toUnix, interval)
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
			log.Debug("DP mergeSeries: %s has multiple series.", series[0].Target)
			for i := range series[0].Datapoints {
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
