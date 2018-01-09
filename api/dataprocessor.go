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

type limiter chan struct{}

func (l limiter) enter() { l <- struct{}{} }
func (l limiter) leave() { <-l }

func newLimiter(l int) limiter {
	return make(chan struct{}, l)
}

type getTargetsResp struct {
	series []models.Series
	err    error
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

// divideContext wraps a Consolidate() call with a context.Context condition
func divideContext(ctx context.Context, pointsA, pointsB []schema.Point) []schema.Point {
	select {
	case <-ctx.Done():
		//request canceled
		return nil
	default:
	}
	return divide(pointsA, pointsB)
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
			remoteReqs[req.Node.GetName()] = append(remoteReqs[req.Node.GetName()], req)
		}
	}

	var wg sync.WaitGroup

	responses := make(chan getTargetsResp, 1)
	getCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if len(localReqs) > 0 {
		wg.Add(1)
		go func() {
			// the only errors returned are from us catching panics, so we should treat them
			// all as internalServerErrors
			series, err := s.getTargetsLocal(getCtx, localReqs)
			if err != nil {
				cancel()
			}
			responses <- getTargetsResp{series, err}
			wg.Done()
		}()
	}
	if len(remoteReqs) > 0 {
		wg.Add(1)
		go func() {
			// all errors returned returned are *response.Error.
			series, err := s.getTargetsRemote(getCtx, remoteReqs)
			if err != nil {
				cancel()
			}
			responses <- getTargetsResp{series, err}
			wg.Done()
		}()
	}

	// wait for all getTargets goroutines to end, then close our responses channel
	go func() {
		wg.Wait()
		close(responses)
	}()

	out := make([]models.Series, 0)
	for resp := range responses {
		if resp.err != nil {
			return nil, resp.err
		}
		out = append(out, resp.series...)
	}
	log.Debug("DP getTargets: %d series found on cluster", len(out))
	return out, nil
}

func (s *Server) getTargetsRemote(ctx context.Context, remoteReqs map[string][]models.Req) ([]models.Series, error) {
	responses := make(chan getTargetsResp, len(remoteReqs))
	rCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg := sync.WaitGroup{}
	wg.Add(len(remoteReqs))
	for _, nodeReqs := range remoteReqs {
		log.Debug("DP getTargetsRemote: handling %d reqs from %s", len(nodeReqs), nodeReqs[0].Node.GetName())
		go func(reqs []models.Req) {
			defer wg.Done()
			node := reqs[0].Node
			buf, err := node.Post(rCtx, "getTargetsRemote", "/getdata", models.GetData{Requests: reqs})
			if err != nil {
				cancel()
				responses <- getTargetsResp{nil, err}
				return
			}
			var resp models.GetDataResp
			_, err = resp.UnmarshalMsg(buf)
			if err != nil {
				cancel()
				log.Error(3, "DP getTargetsRemote: error unmarshaling body from %s/getdata: %q", node.GetName(), err)
				responses <- getTargetsResp{nil, err}
				return
			}
			log.Debug("DP getTargetsRemote: %s returned %d series", node.GetName(), len(resp.Series))
			responses <- getTargetsResp{resp.Series, nil}
		}(nodeReqs)
	}

	// wait for all getTargetsRemote goroutines to end, then close our responses channel
	go func() {
		wg.Wait()
		close(responses)
	}()

	out := make([]models.Series, 0)
	for resp := range responses {
		if resp.err != nil {
			return nil, resp.err
		}
		out = append(out, resp.series...)
	}
	log.Debug("DP getTargetsRemote: total of %d series found on peers", len(out))
	return out, nil
}

// error is the error of the first failing target request
func (s *Server) getTargetsLocal(ctx context.Context, reqs []models.Req) ([]models.Series, error) {
	log.Debug("DP getTargetsLocal: handling %d reqs locally", len(reqs))
	responses := make(chan getTargetsResp, len(reqs))

	var wg sync.WaitGroup
	reqLimiter := newLimiter(getTargetsConcurrency)

	rCtx, cancel := context.WithCancel(ctx)
	defer cancel()
LOOP:
	for _, req := range reqs {
		// check to see if the request has been canceled, if so abort now.
		select {
		case <-rCtx.Done():
			//request canceled
			break LOOP
		default:
		}
		// if there are already getDataConcurrency goroutines running, then block
		// until a slot becomes free.
		reqLimiter.enter()
		wg.Add(1)
		go func(req models.Req) {
			rCtx, span := tracing.NewSpan(rCtx, s.Tracer, "getTargetsLocal")
			req.Trace(span)
			pre := time.Now()
			points, interval, err := s.getTarget(rCtx, req)
			if err != nil {
				tags.Error.Set(span, true)
				cancel() // cancel all other requests.
				responses <- getTargetsResp{nil, err}
			} else {
				getTargetDuration.Value(time.Now().Sub(pre))
				responses <- getTargetsResp{[]models.Series{{
					Target:       req.Target, // always simply the metric name from index
					Datapoints:   points,
					Interval:     interval,
					QueryPatt:    req.Pattern, // foo.* or foo.bar whatever the etName arg was
					QueryFrom:    req.From,
					QueryTo:      req.To,
					QueryCons:    req.ConsReq,
					Consolidator: req.Consolidator,
				}}, nil}
			}
			wg.Done()
			// pop an item of our limiter so that other requests can be processed.
			reqLimiter.leave()
			span.Finish()
		}(req)
	}
	go func() {
		wg.Wait()
		close(responses)
	}()
	out := make([]models.Series, 0, len(reqs))
	for resp := range responses {
		if resp.err != nil {
			return nil, resp.err
		}
		out = append(out, resp.series...)
	}
	log.Debug("DP getTargetsLocal: %d series found locally", len(out))
	return out, nil

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
		fixed, err := s.getSeriesFixed(ctx, req, consolidation.None)
		return fixed, req.OutInterval, err
	} else if !readRollup && normalize {
		fixed, err := s.getSeriesFixed(ctx, req, consolidation.None)
		if err != nil {
			return nil, req.OutInterval, err
		}
		return consolidation.ConsolidateContext(ctx, fixed, req.AggNum, req.Consolidator), req.OutInterval, nil
	} else if readRollup && !normalize {
		if req.Consolidator == consolidation.Avg {
			sumFixed, err := s.getSeriesFixed(ctx, req, consolidation.Sum)
			if err != nil {
				return nil, req.OutInterval, err
			}
			cntFixed, err := s.getSeriesFixed(ctx, req, consolidation.Cnt)
			if err != nil {
				return nil, req.OutInterval, err
			}
			return divideContext(
				ctx,
				sumFixed,
				cntFixed,
			), req.OutInterval, nil
		} else {
			fixed, err := s.getSeriesFixed(ctx, req, consolidation.None)
			return fixed, req.OutInterval, err
		}
	} else {
		// readRollup && normalize
		if req.Consolidator == consolidation.Avg {
			sumFixed, err := s.getSeriesFixed(ctx, req, consolidation.Sum)
			if err != nil {
				return nil, req.OutInterval, err
			}
			cntFixed, err := s.getSeriesFixed(ctx, req, consolidation.Cnt)
			if err != nil {
				return nil, req.OutInterval, err
			}
			return divideContext(
				ctx,
				consolidation.Consolidate(sumFixed, req.AggNum, consolidation.Sum),
				consolidation.Consolidate(cntFixed, req.AggNum, consolidation.Sum),
			), req.OutInterval, nil
		} else {
			fixed, err := s.getSeriesFixed(ctx, req, req.Consolidator)
			if err != nil {
				return nil, req.OutInterval, err
			}
			return consolidation.ConsolidateContext(ctx, fixed, req.AggNum, req.Consolidator), req.OutInterval, nil
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

func (s *Server) getSeriesFixed(ctx context.Context, req models.Req, consolidator consolidation.Consolidator) ([]schema.Point, error) {
	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}
	rctx := newRequestContext(ctx, &req, consolidator)
	res, err := s.getSeries(rctx)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}
	res.Points = append(s.itersToPoints(rctx, res.Iters), res.Points...)
	return Fix(res.Points, req.From, req.To, req.ArchInterval), nil
}

func (s *Server) getSeries(ctx *requestContext) (mdata.Result, error) {
	res := s.getSeriesAggMetrics(ctx)
	select {
	case <-ctx.ctx.Done():
		//request canceled
		return res, nil
	default:
	}

	log.Debug("oldest from aggmetrics is %d", res.Oldest)
	span := opentracing.SpanFromContext(ctx.ctx)
	span.SetTag("oldest_in_ring", res.Oldest)

	if res.Oldest <= ctx.From {
		reqSpanMem.ValueUint32(ctx.To - ctx.From)
		return res, nil
	}

	// if oldest < to -> search until oldest, we already have the rest from mem
	// if to < oldest -> no need to search until oldest, only search until to
	until := util.Min(res.Oldest, ctx.To)
	fromCache, err := s.getSeriesCachedStore(ctx, until)
	if err != nil {
		return res, err
	}
	res.Iters = append(fromCache, res.Iters...)
	return res, nil
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
func (s *Server) getSeriesCachedStore(ctx *requestContext, until uint32) ([]chunk.Iter, error) {
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

	// check to see if the request has been canceled, if so abort now.
	select {
	case <-ctx.ctx.Done():
		//request canceled
		return iters, nil
	default:
	}

	for _, itgen := range cacheRes.Start {
		iter, err := itgen.Get()
		prevts = itgen.Ts
		if err != nil {
			// TODO(replay) figure out what to do if one piece is corrupt
			tracing.Failure(span)
			tracing.Errorf(span, "itergen: error getting iter from Start list %+v", err)
			return iters, err
		}
		iters = append(iters, *iter)
	}

	// check to see if the request has been canceled, if so abort now.
	select {
	case <-ctx.ctx.Done():
		//request canceled
		return iters, nil
	default:
	}

	// the request cannot completely be served from cache, it will require cassandra involvement
	if !cacheRes.Complete {
		if cacheRes.From != cacheRes.Until {
			storeIterGens, err := s.BackendStore.Search(ctx.ctx, key, ctx.Req.TTL, cacheRes.From, cacheRes.Until)
			if err != nil {
				return iters, err
			}
			// check to see if the request has been canceled, if so abort now.
			select {
			case <-ctx.ctx.Done():
				//request canceled
				return iters, nil
			default:
			}

			for _, itgen := range storeIterGens {
				it, err := itgen.Get()
				if err != nil {
					// TODO(replay) figure out what to do if one piece is corrupt
					tracing.Failure(span)
					tracing.Errorf(span, "itergen: error getting iter from cassandra slice %+v", err)
					return iters, err
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
				return iters, err
			}
			iters = append(iters, *it)
		}
	}

	return iters, nil
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
