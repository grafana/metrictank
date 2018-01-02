package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/tracing"
	"github.com/grafana/metrictank/util"
	opentracing "github.com/opentracing/opentracing-go"
	tags "github.com/opentracing/opentracing-go/ext"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
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

// Fix assures all points are nicely aligned (quantized) and padded with nulls in case there's gaps in data
// graphite does this quantization before storing, we may want to do that as well at some point
// note: values are quantized to the right because we can't lie about the future:
// e.g. if interval is 10 and we have a point at 8 or at 2, it will be quantized to 10, we should never move
// values to earlier in time.
func Fix(in []schema.Point, from, to, interval uint32) []schema.Point {
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

func (s *Server) getTargets(ctx context.Context, reqs []models.Req) ([]models.Series, error) {
	// split reqs into local and remote.
	localReqs := make([]models.Req, 0)
	remoteReqs := make(map[string][]models.Req)
	for _, req := range reqs {
		if req.Node.IsLocal() {
			localReqs = append(localReqs, req)
		} else {
			remoteReqs[req.Node.Name] = append(remoteReqs[req.Node.Name], req)
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
			series, err := s.getTargetsLocal(ctx, localReqs)
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
			series, err := s.getTargetsRemote(ctx, remoteReqs)
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

func (s *Server) getTargetsRemote(ctx context.Context, remoteReqs map[string][]models.Req) ([]models.Series, error) {
	seriesChan := make(chan []models.Series, len(remoteReqs))
	errorsChan := make(chan error, len(remoteReqs))
	wg := sync.WaitGroup{}
	wg.Add(len(remoteReqs))
	for _, nodeReqs := range remoteReqs {
		log.Debug("DP getTargetsRemote: handling %d reqs from %s", len(nodeReqs), nodeReqs[0].Node.Name)
		go func(ctx context.Context, reqs []models.Req) {
			defer wg.Done()
			node := reqs[0].Node
			buf, err := node.Post(ctx, "getTargetsRemote", "/getdata", models.GetData{Requests: reqs})
			if err != nil {
				errorsChan <- err
				return
			}
			var resp models.GetDataResp
			_, err = resp.UnmarshalMsg(buf)
			if err != nil {
				log.Error(3, "DP getTargetsRemote: error unmarshaling body from %s/getdata: %q", node.Name, err)
				errorsChan <- err
				return
			}
			log.Debug("DP getTargetsRemote: %s returned %d series", node.Name, len(resp.Series))
			seriesChan <- resp.Series
		}(ctx, nodeReqs)
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
func (s *Server) getTargetsLocal(ctx context.Context, reqs []models.Req) ([]models.Series, error) {
	log.Debug("DP getTargetsLocal: handling %d reqs locally", len(reqs))
	seriesChan := make(chan models.Series, len(reqs))
	errorsChan := make(chan error, len(reqs))
	// TODO: abort pending requests on error, maybe use context, maybe timeouts too
	wg := sync.WaitGroup{}
	wg.Add(len(reqs))
	for _, req := range reqs {
		go func(ctx context.Context, wg *sync.WaitGroup, req models.Req) {
			ctx, span := tracing.NewSpan(ctx, s.Tracer, "getTargetsLocal")
			req.Trace(span)
			defer span.Finish()
			pre := time.Now()
			points, interval, err := s.getTarget(ctx, req)
			if err != nil {
				tags.Error.Set(span, true)
				errorsChan <- err
			} else {
				getTargetDuration.Value(time.Now().Sub(pre))
				seriesChan <- models.Series{
					Target:       req.Target, // always simply the metric name from index
					Datapoints:   points,
					Interval:     interval,
					QueryPatt:    req.Pattern, // foo.* or foo.bar whatever the etName arg was
					QueryFrom:    req.From,
					QueryTo:      req.To,
					QueryCons:    req.ConsReq,
					Consolidator: req.Consolidator,
				}
			}
			wg.Done()
		}(ctx, &wg, req)
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

func (s *Server) getTarget(ctx context.Context, req models.Req) (points []schema.Point, interval uint32, err error) {
	defer doRecover(&err)
	readRollup := req.Archive != 0 // do we need to read from a downsampled series?
	normalize := req.AggNum > 1    // do we need to normalize points at runtime?
	// normalize is runtime consolidation but only for the purpose of bringing high-res
	// series to the same resolution of lower res series.

	if LogLevel < 2 {
		if normalize {
			log.Debug("DP getTarget() %s normalize:true", req.DebugString())
		} else {
			log.Debug("DP getTarget() %s normalize:false", req.DebugString())
		}
	}

	if !readRollup && !normalize {
		return s.getSeriesFixed(ctx, req, consolidation.None), req.OutInterval, nil
	} else if !readRollup && normalize {
		return consolidation.Consolidate(s.getSeriesFixed(ctx, req, consolidation.None), req.AggNum, req.Consolidator), req.OutInterval, nil
	} else if readRollup && !normalize {
		if req.Consolidator == consolidation.Avg {
			return divide(
				s.getSeriesFixed(ctx, req, consolidation.Sum),
				s.getSeriesFixed(ctx, req, consolidation.Cnt),
			), req.OutInterval, nil
		} else {
			return s.getSeriesFixed(ctx, req, req.Consolidator), req.OutInterval, nil
		}
	} else {
		// readRollup && normalize
		if req.Consolidator == consolidation.Avg {
			return divide(
				consolidation.Consolidate(s.getSeriesFixed(ctx, req, consolidation.Sum), req.AggNum, consolidation.Sum),
				consolidation.Consolidate(s.getSeriesFixed(ctx, req, consolidation.Cnt), req.AggNum, consolidation.Sum),
			), req.OutInterval, nil
		} else {
			return consolidation.Consolidate(
				s.getSeriesFixed(ctx, req, req.Consolidator), req.AggNum, req.Consolidator), req.OutInterval, nil
		}
	}
}

func logLoad(typ, key string, from, to uint32) {
	if LogLevel < 2 {
		log.Debug("DP load from %-6s %-20s %d - %d (%s - %s) span:%ds", typ, key, from, to, util.TS(from), util.TS(to), to-from-1)
	}
}

func AggMetricKey(key, archive string, aggSpan uint32) string {
	return fmt.Sprintf("%s_%s_%d", key, archive, aggSpan)
}

func (s *Server) getSeriesFixed(ctx context.Context, req models.Req, consolidator consolidation.Consolidator) []schema.Point {
	rctx := newRequestContext(ctx, &req, consolidator)
	res := s.getSeries(rctx)
	res.Points = append(s.itersToPoints(rctx, res.Iters), res.Points...)
	return Fix(res.Points, req.From, req.To, req.ArchInterval)
}

func (s *Server) getSeries(ctx *requestContext) mdata.Result {
	res := s.getSeriesAggMetrics(ctx)
	log.Debug("oldest from aggmetrics is %d", res.Oldest)
	span := opentracing.SpanFromContext(ctx.ctx)
	span.SetTag("oldest_in_ring", res.Oldest)

	if res.Oldest <= ctx.From {
		reqSpanMem.ValueUint32(ctx.To - ctx.From)
		return res
	}

	// if oldest < to -> search until oldest, we already have the rest from mem
	// if to < oldest -> no need to search until oldest, only search until to
	until := util.Min(res.Oldest, ctx.To)

	res.Iters = append(s.getSeriesCachedStore(ctx, until), res.Iters...)
	return res
}

// getSeries returns points from mem (and cassandra if needed), within the range from (inclusive) - to (exclusive)
// it can query for data within aggregated archives, by using fn min/max/sum/cnt and providing the matching agg span as interval
// pass consolidation.None as consolidator to mean read from raw interval, otherwise we'll read from aggregated series.
// all data will also be quantized.
func (s *Server) itersToPoints(ctx *requestContext, iters []chunk.Iter) []schema.Point {
	pre := time.Now()

	points := pointSlicePool.Get().([]schema.Point)
	for _, iter := range iters {
		total := 0
		good := 0
		for iter.Next() {
			total += 1
			ts, val := iter.Values()
			if ts >= ctx.From && ts < ctx.To {
				good += 1
				points = append(points, schema.Point{Val: val, Ts: ts})
			}
		}
		if LogLevel < 2 {
			log.Debug("DP getSeries: iter %d values good/total %d/%d", iter.T0, good, total)
		}
	}
	itersToPointsDuration.Value(time.Now().Sub(pre))
	return points
}

func (s *Server) getSeriesAggMetrics(ctx *requestContext) mdata.Result {
	_, span := tracing.NewSpan(ctx.ctx, s.Tracer, "getSeriesAggMetrics")
	defer span.Finish()
	metric, ok := s.MemoryStore.Get(ctx.Key)
	if !ok {
		return mdata.Result{
			Oldest: ctx.Req.To,
		}
	}

	if ctx.Cons != consolidation.None {
		logLoad("memory", ctx.AggKey, ctx.From, ctx.To)
		return metric.GetAggregated(ctx.Cons, ctx.Req.ArchInterval, ctx.From, ctx.To)
	} else {
		logLoad("memory", ctx.Req.Key, ctx.From, ctx.To)
		return metric.Get(ctx.From, ctx.To)
	}
}

// will only fetch until until, but uses ctx.To for debug logging
func (s *Server) getSeriesCachedStore(ctx *requestContext, until uint32) []chunk.Iter {
	var iters []chunk.Iter
	var prevts uint32

	key := ctx.Key
	if ctx.Cons != consolidation.None {
		key = ctx.AggKey
	}

	_, span := tracing.NewSpan(ctx.ctx, s.Tracer, "getSeriesCachedStore")
	defer span.Finish()
	span.SetTag("key", key)
	span.SetTag("from", ctx.From)
	span.SetTag("until", until)

	reqSpanBoth.ValueUint32(ctx.To - ctx.From)
	logLoad("cassan", ctx.Key, ctx.From, ctx.To)

	log.Debug("cache: searching query key %s, from %d, until %d", key, ctx.From, until)
	cacheRes := s.Cache.Search(ctx.ctx, key, ctx.From, until)
	log.Debug("cache: result start %d, end %d", len(cacheRes.Start), len(cacheRes.End))

	for _, itgen := range cacheRes.Start {
		iter, err := itgen.Get()
		prevts = itgen.Ts
		if err != nil {
			// TODO(replay) figure out what to do if one piece is corrupt
			tracing.Failure(span)
			tracing.Errorf(span, "itergen: error getting iter from Start list %+v", err)
			continue
		}
		iters = append(iters, *iter)
	}

	// the request cannot completely be served from cache, it will require cassandra involvement
	if !cacheRes.Complete {
		if cacheRes.From != cacheRes.Until {
			storeIterGens, err := s.BackendStore.Search(ctx.ctx, key, ctx.Req.TTL, cacheRes.From, cacheRes.Until)
			if err != nil {
				panic(err)
			}

			for _, itgen := range storeIterGens {
				it, err := itgen.Get()
				if err != nil {
					// TODO(replay) figure out what to do if one piece is corrupt
					tracing.Failure(span)
					tracing.Errorf(span, "itergen: error getting iter from cassandra slice %+v", err)
					continue
				}
				// it's important that the itgens get added in chronological order,
				// currently we rely on cassandra returning results in order
				s.Cache.Add(key, ctx.Key, prevts, itgen)
				prevts = itgen.Ts
				iters = append(iters, *it)
			}
		}

		// the End slice is in reverse order
		for i := len(cacheRes.End) - 1; i >= 0; i-- {
			it, err := cacheRes.End[i].Get()
			if err != nil {
				// TODO(replay) figure out what to do if one piece is corrupt
				log.Error(3, "itergen: error getting iter from cache result end slice %+v", err)
				continue
			}
			iters = append(iters, *it)
		}
	}

	return iters
}

// check for duplicate series names for the same query. If found merge the results.
func mergeSeries(in []models.Series) []models.Series {
	type segment struct {
		target string
		query  string
		from   uint32
		to     uint32
		con    consolidation.Consolidator
	}
	seriesByTarget := make(map[segment][]models.Series)
	for _, series := range in {
		s := segment{
			series.Target,
			series.QueryPatt,
			series.QueryFrom,
			series.QueryTo,
			series.Consolidator,
		}
		seriesByTarget[s] = append(seriesByTarget[s], series)
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

type requestContext struct {
	ctx context.Context

	// request by external user.
	Req *models.Req

	// internal request needed to satisfy user request.
	Cons   consolidation.Consolidator // to satisfy avg request from user, this would be sum or cnt
	From   uint32                     // may be different than user request, see below
	To     uint32                     // may be different than user request, see below
	Key    string                     // key to query
	AggKey string                     // aggkey to query (if needed)
}

func prevBoundary(ts uint32, span uint32) uint32 {
	return ts - ((ts-1)%span + 1)
}

func newRequestContext(ctx context.Context, req *models.Req, consolidator consolidation.Consolidator) *requestContext {

	rc := requestContext{
		ctx:  ctx,
		Req:  req,
		Cons: consolidator,
		Key:  req.Key,
	}

	// while aggregated archives are quantized, raw intervals are not.  quantizing happens after fetching the data,
	// So we have to adjust the range to get the right data.
	// (ranges described as a..b include both and b)
	// REQ           0[---FROM---60]----------120-----------180[----TO----240]  any request from 1..60 to 181..240 should ...
	// QUANTD RESULT 0----------[60]---------[120]---------[180]                return points 60, 120 and 180 (simply because of to/from and inclusive/exclusive rules) ..
	// STORED DATA   0[----------60][---------120][---------180][---------240]  but data for 60 may be at 1..60, data for 120 at 61..120 and for 180 at 121..180 (due to quantizing)
	// to retrieve the stored data, we also use from inclusive and to exclusive,
	// so to make sure that the data after quantization (Fix()) is correct, we have to make the following adjustment:
	// `from`   1..60 needs data    1..60   -> always adjust `from` to previous boundary+1 (here 1)
	// `to`  181..240 needs data 121..180   -> always adjust `to`   to previous boundary+1 (here 181)

	if consolidator == consolidation.None {
		rc.From = prevBoundary(req.From, req.ArchInterval) + 1
		rc.To = prevBoundary(req.To, req.ArchInterval) + 1
	} else {
		rc.From = req.From
		rc.To = req.To
		rc.AggKey = AggMetricKey(req.Key, consolidator.Archive(), req.ArchInterval)
	}

	return &rc
}
