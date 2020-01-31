package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/schema"
	macaron "gopkg.in/macaron.v1"

	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/expr"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/tracing"
	"github.com/grafana/metrictank/util"
	opentracing "github.com/opentracing/opentracing-go"
	tags "github.com/opentracing/opentracing-go/ext"
	traceLog "github.com/opentracing/opentracing-go/log"
	"github.com/raintank/dur"
	log "github.com/sirupsen/logrus"
)

var MissingOrgHeaderErr = errors.New("orgId not set in headers")
var MissingQueryErr = errors.New("missing query param")
var InvalidFormatErr = errors.New("invalid format specified")
var InvalidTimeRangeErr = errors.New("invalid time range requested")
var renderReqProxied = stats.NewCounter32("api.request.render.proxied")

var (
	// metric api.request.render.series is the number of series a /render request is handling.  This is the number
	// of metrics after all of the targets in the request have expanded by searching the index.
	reqRenderSeriesCount = stats.NewMeter32("api.request.render.series", false)

	// metric api.request.render.targets is the number of targets a /render request is handling.
	reqRenderTargetCount = stats.NewMeter32("api.request.render.targets", false)

	// metric plan.run is the time spent running the plan for a request (function processing of all targets and runtime consolidation)
	planRunDuration = stats.NewLatencyHistogram15s32("plan.run")
)

// map of consolidation methods and the ordered list of rollup aggregations that should
// be used. e.g. if a user requests 'min' but all we have is 'avg' and 'sum' then use 'avg'.
var rollupPreference = map[consolidation.Consolidator][]consolidation.Consolidator{
	consolidation.Avg: {
		consolidation.Avg,
		consolidation.Lst,
		consolidation.Max,
		consolidation.Min,
		consolidation.Sum,
	},
	consolidation.Min: {
		consolidation.Min,
		consolidation.Lst,
		consolidation.Avg,
		consolidation.Max,
		consolidation.Sum,
	},
	consolidation.Max: {
		consolidation.Max,
		consolidation.Lst,
		consolidation.Avg,
		consolidation.Min,
		consolidation.Sum,
	},
	consolidation.Sum: {
		consolidation.Sum,
		consolidation.Lst,
		consolidation.Avg,
		consolidation.Max,
		consolidation.Min,
	},
	consolidation.Lst: {
		consolidation.Lst,
		consolidation.Avg,
		consolidation.Max,
		consolidation.Min,
		consolidation.Sum,
	},
}

type Series struct {
	Pattern string // pattern used for index lookup. typically user input like foo.{b,a}r.*
	Series  []idx.Node
	Node    cluster.Node
}

func (s *Server) findSeries(ctx context.Context, orgId uint32, patterns []string, seenAfter int64) ([]Series, error) {
	data := models.IndexFind{
		Patterns: patterns,
		OrgId:    orgId,
		From:     seenAfter,
	}

	resps, err := s.peerQuerySpeculative(ctx, data, "findSeriesRemote", "/index/find")
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}

	series := make([]Series, 0)
	resp := models.IndexFindResp{}
	for _, r := range resps {
		_, err = resp.UnmarshalMsg(r.buf)
		if err != nil {
			return nil, err
		}

		for pattern, nodes := range resp.Nodes {
			series = append(series, Series{
				Pattern: pattern,
				Node:    r.peer,
				Series:  nodes,
			})
			log.Debugf("HTTP findSeries %d matches for %s found on %s", len(nodes), pattern, r.peer.GetName())
		}
	}

	return series, nil
}

func (s *Server) proxyToGraphite(ctx *middleware.Context) {
	proxyCtx, span := tracing.NewSpan(ctx.Req.Context(), s.Tracer, "graphiteproxy")
	tags.SpanKindRPCClient.Set(span)
	tags.PeerService.Set(span, "graphite")
	ctx.Req = macaron.Request{ctx.Req.WithContext(proxyCtx)}
	ctx.Req.Request.Body = ctx.Body
	carrier := opentracing.HTTPHeadersCarrier(ctx.Req.Header)
	err := s.Tracer.Inject(span.Context(), opentracing.HTTPHeaders, carrier)
	if err != nil {
		log.Errorf("HTTP renderMetrics failed to inject span into headers: %s", err.Error())
	}
	graphiteProxy.ServeHTTP(ctx.Resp, ctx.Req.Request)
	if span != nil {
		span.Finish()
	}
	renderReqProxied.Inc()
}

func (s *Server) renderMetrics(ctx *middleware.Context, request models.GraphiteRender) {
	// retrieve te span that was created by the api Middleware handler.  The handler will
	// call span.Finish()
	span := opentracing.SpanFromContext(ctx.Req.Context())

	span.SetTag("orgid", ctx.OrgId)

	// note: the model is already validated to assure at least one of them has len >0
	if len(request.Targets) == 0 {
		request.Targets = request.TargetsRails
	}

	// log information about the request.
	span.LogFields(
		traceLog.String("from", request.FromTo.From),
		traceLog.String("until", request.FromTo.Until),
		traceLog.String("to", request.FromTo.To),
		traceLog.String("tz", request.FromTo.Tz),
		traceLog.Int32("mdp", int32(request.MaxDataPoints)),
		traceLog.String("targets", fmt.Sprintf("%q", request.Targets)),
		traceLog.String("format", request.Format),
		traceLog.Bool("noproxy", request.NoProxy),
		traceLog.String("process", request.Process),
	)

	now := time.Now()
	defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	defaultTo := uint32(now.Unix())
	fromUnix, toUnix, err := getFromTo(request.FromTo, now, defaultFrom, defaultTo)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	if fromUnix >= toUnix {
		response.Write(ctx, response.NewError(http.StatusBadRequest, InvalidTimeRangeErr.Error()))
		return
	}

	span.LogFields(
		traceLog.Int32("fromUnix", int32(fromUnix)),
		traceLog.Int32("toUnix", int32(toUnix)),
		traceLog.Int32("span", int32(toUnix-fromUnix)),
	)

	// render API is modeled after graphite, so from exclusive, to inclusive.
	// in MT, from is inclusive, to is exclusive (which is akin to slice syntax)
	// so we must adjust
	fromUnix++
	toUnix++

	exprs, err := expr.ParseMany(request.Targets)
	if err != nil {
		ctx.Error(http.StatusBadRequest, err.Error())
		return
	}

	reqRenderTargetCount.Value(len(request.Targets))

	if request.Process == "none" {
		s.proxyToGraphite(ctx)
		return
	}

	stable := request.Process == "stable"
	mdp := request.MaxDataPoints
	if request.NoProxy {
		// if this request is coming from graphite, we should not do runtime consolidation
		// as graphite needs high-res data to perform its processing.
		mdp = 0
	}

	opts, err := optimizations.ApplyUserPrefs(request.Optimizations)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	plan, err := expr.NewPlan(exprs, fromUnix, toUnix, mdp, stable, opts)
	if err != nil {
		if fun, ok := err.(expr.ErrUnknownFunction); ok {
			if request.NoProxy {
				ctx.Error(http.StatusBadRequest, "localOnly requested, but the request cant be handled locally")
				return
			}
			s.proxyToGraphite(ctx)
			proxyStats.Miss(string(fun))
			return
		}
		ctx.Error(http.StatusBadRequest, err.Error())
		return
	}

	execCtx, execSpan := tracing.NewSpan(ctx.Req.Context(), s.Tracer, "executePlan")
	defer execSpan.Finish()
	out, meta, err := s.executePlan(execCtx, ctx.OrgId, plan)
	if err != nil {
		err := response.WrapError(err)
		if err.Code() != http.StatusBadRequest {
			tracing.Failure(execSpan)
		}
		tracing.Error(execSpan, err)
		response.Write(ctx, err)
		return
	}

	// check to see if the request has been canceled, if so abort now.
	select {
	case <-execCtx.Done():
		//request canceled
		response.Write(ctx, response.RequestCanceledErr)
		return
	default:
	}

	noDataPoints := true
	for _, o := range out {
		if len(o.Datapoints) != 0 {
			noDataPoints = false
		}
	}
	if noDataPoints {
		span.SetTag("nodatapoints", true)
	}

	switch request.Format {
	case "msgp":
		response.Write(ctx, response.NewMsgp(200, models.SeriesByTarget(out)))
	case "msgpack":
		response.Write(ctx, response.NewMsgpack(200, models.SeriesByTarget(out).ForGraphite("msgpack")))
	case "pickle":
		response.Write(ctx, response.NewPickle(200, models.SeriesByTarget(out)))
	default:
		if request.Meta {
			response.Write(ctx, response.NewFastJson(200, models.ResponseWithMeta{Series: models.SeriesByTarget(out), Meta: meta}))
		} else {
			response.Write(ctx, response.NewFastJson(200, models.SeriesByTarget(out)))
		}
	}
	plan.Clean()
}

func (s *Server) metricsFind(ctx *middleware.Context, request models.GraphiteFind) {
	now := time.Now()
	var defaultFrom, defaultTo uint32
	fromUnix, toUnix, err := getFromTo(request.FromTo, now, defaultFrom, defaultTo)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	nodes := make([]idx.Node, 0)
	reqCtx := ctx.Req.Context()
	series, err := s.findSeries(reqCtx, ctx.OrgId, []string{request.Query}, int64(fromUnix))
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	// check to see if the request has been canceled, if so abort now.
	select {
	case <-reqCtx.Done():
		//request canceled
		response.Write(ctx, response.RequestCanceledErr)
		return
	default:
	}

	seenPaths := make(map[string]struct{})
	// different nodes may have overlapping data in their index.
	// maybe because they used to receive a certain shard but now dont. or because they host metrics under branches
	// that other nodes also host metrics under. It may even happen that a node has a leaf that for another
	// node is a branch, if the org has been sending improper data.  in this case there's no elegant way
	// to nicely handle this so we'll just ignore one of them like we ignore other paths we've already seen.
	for _, s := range series {
		for _, n := range s.Series {
			if _, ok := seenPaths[n.Path]; !ok {
				nodes = append(nodes, n)
				seenPaths[n.Path] = struct{}{}
			}
		}
	}

	switch request.Format {
	case "", "treejson", "json":
		response.Write(ctx, response.NewJson(200, findTreejson(request.Query, nodes), request.Jsonp))
	case "completer":
		response.Write(ctx, response.NewJson(200, findCompleter(nodes), request.Jsonp))
	case "msgpack":
		response.Write(ctx, response.NewMsgpack(200, findPickle(nodes, request, fromUnix, toUnix)))
	case "pickle":
		response.Write(ctx, response.NewPickle(200, findPickle(nodes, request, fromUnix, toUnix)))
	}
}

func (s *Server) listLocal(orgId uint32) []idx.Archive {

	// query nodes have no data
	if s.MetricIndex == nil {
		return nil
	}

	return s.MetricIndex.List(orgId)
}

func (s *Server) listRemote(ctx context.Context, orgId uint32, peer cluster.Node) ([]idx.Archive, error) {
	log.Debugf("HTTP IndexJson() querying %s/index/list for %d", peer.GetName(), orgId)
	buf, err := peer.Post(ctx, "listRemote", "/index/list", models.IndexList{OrgId: orgId})
	if err != nil {
		log.Errorf("HTTP IndexJson() error querying %s/index/list: %q", peer.GetName(), err.Error())
		return nil, err
	}
	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}
	result := make([]idx.Archive, 0)
	for len(buf) != 0 {
		var def idx.Archive
		buf, err = def.UnmarshalMsg(buf)
		if err != nil {
			log.Errorf("HTTP IndexJson() error unmarshaling body from %s/index/list: %q", peer.GetName(), err.Error())
			return nil, err
		}
		result = append(result, def)
	}
	return result, nil
}

func (s *Server) metricsIndex(ctx *middleware.Context) {
	peers, err := cluster.MembersForQuery()
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}
	reqCtx, cancel := context.WithCancel(ctx.Req.Context())
	defer cancel()
	responses := make(chan struct {
		series []idx.Archive
		err    error
	}, 1)
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		if peer.IsLocal() {
			go func() {
				result := s.listLocal(ctx.OrgId)
				responses <- struct {
					series []idx.Archive
					err    error
				}{result, nil}
				wg.Done()
			}()
		} else {
			go func(peer cluster.Node) {
				result, err := s.listRemote(reqCtx, ctx.OrgId, peer)
				if err != nil {
					cancel()
				}
				responses <- struct {
					series []idx.Archive
					err    error
				}{result, err}
				wg.Done()
			}(peer)
		}
	}

	// wait for all list goroutines to end, then close our responses channel
	go func() {
		wg.Wait()
		close(responses)
	}()

	series := make([]idx.Archive, 0)
	seenDefs := make(map[schema.MKey]struct{})
	for resp := range responses {
		if resp.err != nil {
			response.Write(ctx, response.WrapError(err))
			return
		}
		for _, def := range resp.series {
			if _, ok := seenDefs[def.Id]; !ok {
				series = append(series, def)
				seenDefs[def.Id] = struct{}{}
			}
		}
	}

	// check to see if the request has been canceled, if so abort now.
	select {
	case <-reqCtx.Done():
		//request canceled
		response.Write(ctx, response.RequestCanceledErr)
		return
	default:
	}

	response.Write(ctx, response.NewFastJson(200, models.MetricNames(series)))
}

func findCompleter(nodes []idx.Node) models.SeriesCompleter {
	var result = models.NewSeriesCompleter()
	for _, g := range nodes {
		c := models.SeriesCompleterItem{
			Path: string(g.Path),
		}

		if g.Leaf {
			c.IsLeaf = "1"
		} else {
			c.IsLeaf = "0"
		}

		i := strings.Index(c.Path, ";")
		if i == -1 {
			i = len(c.Path)
		}
		i = strings.LastIndex(c.Path[:i], ".")

		if i != -1 {
			c.Name = c.Path[i+1:]
		}
		result.Add(c)
	}

	return result
}

func findPickle(nodes []idx.Node, request models.GraphiteFind, fromUnix, toUnix uint32) models.SeriesPickle {
	result := make([]models.SeriesPickleItem, len(nodes))
	var intervals [][]int64
	if fromUnix != 0 && toUnix != 0 {
		intervals = [][]int64{{int64(fromUnix), int64(toUnix)}}
	}
	for i, g := range nodes {
		result[i] = models.NewSeriesPickleItem(g.Path, g.Leaf, intervals)
	}
	return result
}

var treejsonContext = make(map[string]int)

func findTreejson(query string, nodes []idx.Node) models.SeriesTree {
	tree := models.SeriesTree{}
	seen := make(map[string]struct{})

	for _, g := range nodes {
		name := string(g.Path)
		i := strings.Index(name, ";")
		if i == -1 {
			i = len(name)
		}
		if i = strings.LastIndex(name[:i], "."); i != -1 {
			name = name[i+1:]
		}

		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		allowChildren := 0
		leaf := 0
		expandable := 0
		if g.HasChildren {
			allowChildren = 1
			expandable = 1
		}
		if g.Leaf {
			leaf = 1
		}

		t := models.SeriesTreeItem{
			ID:            g.Path,
			Context:       treejsonContext,
			Text:          name,
			AllowChildren: allowChildren,
			Expandable:    expandable,
			Leaf:          leaf,
		}
		tree.Add(&t)
	}
	return tree
}

func (s *Server) metricsDelete(ctx *middleware.Context, req models.MetricsDelete) {
	peers := cluster.Manager.MemberList(false, true)
	peers = append(peers, cluster.Manager.ThisNode())
	log.Debugf("HTTP metricsDelete for %v across %d instances", req.Query, len(peers))

	reqCtx, cancel := context.WithCancel(ctx.Req.Context())
	defer cancel()
	deleted := 0
	responses := make(chan struct {
		deleted int
		err     error
	}, len(peers))
	var wg sync.WaitGroup
	for _, peer := range peers {
		log.Debugf("HTTP metricsDelete getting results from %s", peer.GetName())
		wg.Add(1)
		if peer.IsLocal() {
			go func() {
				result, err := s.metricsDeleteLocal(ctx.OrgId, req.Query)
				var e error
				if err != nil {
					cancel()
					if strings.Contains(err.Error(), "Index is corrupt") {
						e = response.NewError(http.StatusInternalServerError, err.Error())
					} else {
						e = response.NewError(http.StatusBadRequest, err.Error())
					}
				}
				responses <- struct {
					deleted int
					err     error
				}{result, e}
				wg.Done()
			}()
		} else {
			go func(peer cluster.Node) {
				result, err := s.metricsDeleteRemote(reqCtx, ctx.OrgId, req.Query, peer)
				if err != nil {
					cancel()
				}
				responses <- struct {
					deleted int
					err     error
				}{result, err}
				wg.Done()
			}(peer)
		}
	}

	// wait for all metricsDelete goroutines to end, then close our responses channel
	go func() {
		wg.Wait()
		close(responses)
	}()

	for resp := range responses {
		if resp.err != nil {
			response.Write(ctx, response.WrapError(resp.err))
			return
		}
		deleted += resp.deleted
	}

	// check to see if the request has been canceled, if so abort now.
	select {
	case <-reqCtx.Done():
		//request canceled
		response.Write(ctx, response.RequestCanceledErr)
		return
	default:
	}

	resp := models.MetricsDeleteResp{
		DeletedDefs: deleted,
	}

	response.Write(ctx, response.NewJson(200, resp, ""))
}

func (s *Server) metricsDeleteLocal(orgId uint32, query string) (int, error) {

	// nothing to do on query nodes.
	if s.MetricIndex == nil {
		return 0, nil
	}

	if tagquery.IsSeriesByTagExpression(query) {
		exprs, err := tagquery.ParseSeriesByTagExpression(query)
		if err != nil {
			return 0, err
		}
		q, err := tagquery.NewQuery(exprs, 0)
		if err != nil {
			return 0, err
		}
		defs := s.MetricIndex.DeleteTagged(orgId, q)
		return len(defs), err

	} else {
		defs, err := s.MetricIndex.Delete(orgId, query)
		return len(defs), err
	}
}

func (s *Server) metricsDeleteRemote(ctx context.Context, orgId uint32, query string, peer cluster.Node) (int, error) {
	log.Debugf("HTTP metricDelete calling %s/index/delete for %d:%q", peer.GetName(), orgId, query)

	body := models.IndexDelete{
		Query: query,
		OrgId: orgId,
	}
	buf, err := peer.Post(ctx, "metricsDeleteRemote", "/index/delete", body)
	if err != nil {
		log.Errorf("HTTP metricDelete error querying %s/index/delete: %q", peer.GetName(), err.Error())
		return 0, err
	}

	select {
	case <-ctx.Done():
		//request canceled
		return 0, nil
	default:
	}

	resp := models.MetricsDeleteResp{}
	_, err = resp.UnmarshalMsg(buf)
	if err != nil {
		log.Errorf("HTTP metricDelete error unmarshaling body from %s/index/delete: %q", peer.GetName(), err.Error())
		return 0, err
	}

	return resp.DeletedDefs, nil
}

// executePlan looks up the needed data, retrieves it, and then invokes the processing
// note if you do something like sum(foo.*) and all of those metrics happen to be on another node,
// we will collect all the individual series from the peer, and then sum here. that could be optimized
func (s *Server) executePlan(ctx context.Context, orgId uint32, plan expr.Plan) ([]models.Series, models.RenderMeta, error) {
	var meta models.RenderMeta

	minFrom := uint32(math.MaxUint32)
	var maxTo uint32
	reqs := NewReqMap()
	metaTagEnrichmentData := make(map[string]tagquery.Tags)

	// note that different patterns to query can have different from / to, so they require different index lookups
	// e.g. target=movingAvg(foo.*, "1h")&target=foo.*
	// note that in this case we fetch foo.* twice. can be optimized later
	pre := time.Now()
	for _, r := range plan.Reqs {
		select {
		case <-ctx.Done():
			//request canceled
			return nil, meta, nil
		default:
		}
		var err error
		var series []Series
		var exprs tagquery.Expressions
		if tagquery.IsSeriesByTagExpression(r.Query) {
			exprs, err = tagquery.ParseSeriesByTagExpression(r.Query)
			if err != nil {
				return nil, meta, err
			}
			series, err = s.clusterFindByTag(ctx, orgId, exprs, int64(r.From), maxSeriesPerReq-int(reqs.cnt), false)
		} else {
			series, err = s.findSeries(ctx, orgId, []string{r.Query}, int64(r.From))
		}
		if err != nil {
			return nil, meta, err
		}

		minFrom = util.Min(minFrom, r.From)
		maxTo = util.Max(maxTo, r.To)

		for _, s := range series {
			for _, metric := range s.Series {
				for _, archive := range metric.Defs {
					var cons consolidation.Consolidator
					consReq := r.Cons
					if consReq == 0 {
						// we will use the primary method dictated by the storage-aggregations rules
						// note:
						// * we can't just let the expr library take care of normalization, as we may have to fetch targets
						//   from cluster peers; it's more efficient to have them normalize the data at the source.
						// * a pattern may expand to multiple series, each of which can have their own aggregation method.
						fn := mdata.Aggregations.Get(archive.AggId).AggregationMethod[0]
						cons = consolidation.Consolidator(fn) // we use the same number assignments so we can cast them
					} else {
						// user specified a runtime consolidation function via consolidateBy()
						// get the consolidation method of the most appropriate rollup based on the consolidation method
						// requested by the user.  e.g. if the user requested 'min' but we only have 'avg' and 'sum' rollups,
						// use 'avg'.
						cons = closestAggMethod(consReq, mdata.Aggregations.Get(archive.AggId).AggregationMethod)
					}

					newReq := r.ToModel()
					newReq.Init(archive, cons, s.Node)
					reqs.Add(newReq)
				}

				if tagquery.MetaTagSupport && len(metric.Defs) > 0 && len(metric.MetaTags) > 0 {
					metaTagEnrichmentData[metric.Defs[0].NameWithTags()] = metric.MetaTags
				}
			}
		}
	}

	meta.RenderStats.ResolveSeriesDuration = time.Since(pre)

	select {
	case <-ctx.Done():
		//request canceled
		return nil, meta, nil
	default:
	}

	reqRenderSeriesCount.ValueUint32(reqs.cnt)
	if reqs.cnt == 0 {
		return nil, meta, nil
	}

	meta.RenderStats.SeriesFetch = reqs.cnt

	// note: if 1 series has a movingAvg that requires a long time range extension, it may push other reqs into another archive. can be optimized later
	var err error
	var rp *ReqsPlan
	rp, err = planRequests(uint32(time.Now().Unix()), minFrom, maxTo, reqs, plan.MaxDataPoints, maxPointsPerReqSoft, maxPointsPerReqHard)
	if err != nil {
		return nil, meta, err
	}
	meta.RenderStats.PointsFetch = rp.PointsFetch()
	meta.RenderStats.PointsReturn = rp.PointsReturn(plan.MaxDataPoints)
	reqsList := rp.List()

	span := opentracing.SpanFromContext(ctx)
	span.SetTag("num_reqs", len(reqsList))
	span.SetTag("points_fetch", meta.RenderStats.PointsFetch)
	span.SetTag("points_return", meta.RenderStats.PointsReturn)

	for _, req := range reqsList {
		log.Debugf("HTTP Render %s - arch:%d archI:%d outI:%d aggN: %d from %s", req, req.Archive, req.ArchInterval, req.OutInterval, req.AggNum, req.Node.GetName())
	}

	a := time.Now()
	out, err := s.getTargets(ctx, &meta.StorageStats, reqsList)
	if err != nil {
		log.Errorf("HTTP Render %s", err.Error())
		return nil, meta, err
	}
	b := time.Now()
	meta.RenderStats.GetTargetsDuration = b.Sub(a)
	meta.StorageStats.Trace(span)

	out = mergeSeries(out)

	if len(metaTagEnrichmentData) > 0 {
		for i := range out {
			if metaTags, ok := metaTagEnrichmentData[out[i].Target]; ok {
				out[i].EnrichWithTags(metaTags)
			}
		}
	}

	// instead of waiting for all data to come in and then start processing everything, we could consider starting processing earlier, at the risk of doing needless work
	// if we need to cancel the request due to a fetch error

	data := make(map[expr.Req][]models.Series)
	for _, serie := range out {
		q := expr.NewReqFromSerie(serie)
		data[q] = append(data[q], serie)
	}

	// Sort each merged series so that the output of a function is well-defined and repeatable.
	for k := range data {
		sort.Sort(models.SeriesByTarget(data[k]))
	}
	meta.RenderStats.PrepareSeriesDuration = time.Since(b)

	preRun := time.Now()
	out, err = plan.Run(data)

	meta.RenderStats.PlanRunDuration = time.Since(preRun)
	planRunDuration.Value(meta.RenderStats.PlanRunDuration)
	return out, meta, err
}

// find the best consolidation method based on what was requested and what aggregations are available.
func closestAggMethod(requested consolidation.Consolidator, available []conf.Method) consolidation.Consolidator {
	// if there is only 1 consolidation method available, then that is all we can return.
	if len(available) == 1 {
		return consolidation.Consolidator(available[0])
	}

	avail := map[consolidation.Consolidator]struct{}{}
	for _, a := range available {
		avail[consolidation.Consolidator(a)] = struct{}{}
	}
	var orderOfPreference []consolidation.Consolidator
	orderOfPreference, ok := rollupPreference[requested]
	if !ok {
		return consolidation.Consolidator(available[0])
	}
	for _, p := range orderOfPreference {
		if _, ok := avail[p]; ok {
			return p
		}
	}
	// fall back to the default aggregation method.
	return consolidation.Consolidator(available[0])
}

func getFromTo(ft models.FromTo, now time.Time, defaultFrom, defaultTo uint32) (uint32, uint32, error) {
	loc, err := getLocation(ft.Tz)
	if err != nil {
		return 0, 0, err
	}

	from := ft.From
	to := ft.To
	if to == "" {
		to = ft.Until
	}

	fromUnix, err := dur.ParseDateTime(from, loc, now, defaultFrom)
	if err != nil {
		return 0, 0, err
	}

	toUnix, err := dur.ParseDateTime(to, loc, now, defaultTo)
	if err != nil {
		return 0, 0, err
	}

	return fromUnix, toUnix, nil
}

func getLocation(desc string) (*time.Location, error) {
	switch desc {
	case "":
		return timeZone, nil
	case "local":
		return time.Local, nil
	}
	return time.LoadLocation(desc)
}

func (s *Server) graphiteTagDetails(ctx *middleware.Context, request models.GraphiteTagDetails) {
	tag := ctx.Params(":tag")
	if len(tag) <= 0 {
		response.Write(ctx, response.NewError(http.StatusBadRequest, "not tag specified"))
		return
	}
	reqCtx := ctx.Req.Context()
	tagValues, err := s.clusterTagDetails(reqCtx, ctx.OrgId, tag, request.Filter)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	select {
	case <-reqCtx.Done():
		//request canceled
		response.Write(ctx, response.RequestCanceledErr)
		return
	default:
	}

	resp := models.GraphiteTagDetailsResp{
		Tag:    tag,
		Values: make([]models.GraphiteTagDetailsValueResp, 0, len(tagValues)),
	}

	for k, v := range tagValues {
		resp.Values = append(resp.Values, models.GraphiteTagDetailsValueResp{
			Value: k,
			Count: v,
		})
	}

	response.Write(ctx, response.NewJson(200, resp, ""))
}

func (s *Server) clusterTagDetails(ctx context.Context, orgId uint32, tag, filter string) (map[string]uint64, error) {
	result := make(map[string]uint64)

	data := models.IndexTagDetails{OrgId: orgId, Tag: tag, Filter: filter}
	resps, err := s.peerQuerySpeculative(ctx, data, "clusterTagDetails", "/index/tag_details")
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}
	resp := models.IndexTagDetailsResp{}
	for _, r := range resps {
		_, err = resp.UnmarshalMsg(r.buf)
		if err != nil {
			return nil, err
		}
		for k, v := range resp.Values {
			result[k] = result[k] + v
		}
	}

	return result, nil
}

func (s *Server) graphiteTagFindSeries(ctx *middleware.Context, request models.GraphiteTagFindSeries) {
	reqCtx := ctx.Req.Context()

	expressions, err := tagquery.ParseExpressions(request.Expr)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	// Out of the provided soft limit and the global `maxSeriesPerReq` hard limit
	// (either of which may be 0 aka disabled), pick the only one that matters: the most strict one.
	limit := request.Limit
	isSoftLimit := limit > 0
	if maxSeriesPerReq > 0 && (limit == 0 || limit > maxSeriesPerReq) {
		limit = maxSeriesPerReq
		isSoftLimit = false
	}

	series, err := s.clusterFindByTag(reqCtx, ctx.OrgId, expressions, request.From, limit, isSoftLimit)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	select {
	case <-reqCtx.Done():
		//request canceled
		response.Write(ctx, response.RequestCanceledErr)
		return
	default:
	}

	var warnings []string
	if len(series) == limit {
		warnings = append(warnings, "Result set truncated due to limit")
	}

	switch request.Format {
	case "lastts-json":
		retval := models.GraphiteTagFindSeriesLastTsResp{Warnings: warnings}
		retval.Series = make([]models.SeriesLastTs, 0, len(series))
		for _, serie := range series {
			var lastUpdate int64
			for _, node := range serie.Series {
				for _, ndef := range node.Defs {
					if ndef.LastUpdate > lastUpdate {
						lastUpdate = ndef.LastUpdate
					}
				}
			}
			retval.Series = append(retval.Series, models.SeriesLastTs{Series: serie.Pattern, Ts: lastUpdate})
		}

		response.Write(ctx, response.NewJson(200, retval, ""))
	case "series-json":
		seriesNames := make([]string, 0, len(series))
		for _, serie := range series {
			// note: for findByTag the "Pattern" is the full metric nameWithTags
			seriesNames = append(seriesNames, serie.Pattern)
		}

		if request.Meta == true {
			retval := models.GraphiteTagFindSeriesMetaResp{Series: seriesNames, Warnings: warnings}
			response.Write(ctx, response.NewJson(200, retval, ""))
		} else {
			response.Write(ctx, response.NewJson(200, seriesNames, ""))
		}
	}
}

// clusterFindByTag returns the Series matching the given expressions.
// If maxSeries is > 0, it specifies a limit which will truncate the resultset (if softLimit is true) or return an error otherwise.
func (s *Server) clusterFindByTag(ctx context.Context, orgId uint32, expressions tagquery.Expressions, from int64, maxSeries int, softLimit bool) ([]Series, error) {
	data := models.IndexFindByTag{OrgId: orgId, Expr: expressions.Strings(), From: from}
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	responseChan, errorChan := s.peerQuerySpeculativeChan(newCtx, data, "clusterFindByTag", "/index/find_by_tag")

	var allSeries []Series

	for r := range responseChan {
		resp := models.IndexFindByTagResp{}
		_, err := resp.UnmarshalMsg(r.buf)
		if err != nil {
			return nil, err
		}

		// Only check if maxSeriesPerReq > 0 (meaning enabled) or soft-limited
		checkSeriesLimit := maxSeriesPerReq > 0 || softLimit
		if checkSeriesLimit && len(resp.Metrics)+len(allSeries) > maxSeries {
			if softLimit {
				remainingSpace := maxSeries - len(allSeries)
				// Fill in up to maxSeries
				for _, series := range resp.Metrics[:remainingSpace] {
					allSeries = append(allSeries, Series{
						Pattern: series.Path,
						Node:    r.peer,
						Series:  []idx.Node{series},
					})
				}
				return allSeries, nil
			}
			return nil,
				response.NewError(
					http.StatusRequestEntityTooLarge,
					fmt.Sprintf("Request exceeds max-series-per-req limit (%d). Reduce the number of targets or ask your admin to increase the limit.", maxSeriesPerReq))
		}

		for _, series := range resp.Metrics {
			allSeries = append(allSeries, Series{
				Pattern: series.Path,
				Node:    r.peer,
				Series:  []idx.Node{series},
			})
		}
	}

	err := <-errorChan
	return allSeries, err
}

func (s *Server) graphiteTags(ctx *middleware.Context, request models.GraphiteTags) {
	reqCtx := ctx.Req.Context()
	tags, err := s.clusterTags(reqCtx, ctx.OrgId, request.Filter)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	select {
	case <-reqCtx.Done():
		//request canceled
		response.Write(ctx, response.RequestCanceledErr)
		return
	default:
	}

	resp := make(models.GraphiteTagsResp, 0)
	for _, tag := range tags {
		resp = append(resp, models.GraphiteTagResp{Tag: tag})
	}
	response.Write(ctx, response.NewJson(200, resp, ""))
}

func (s *Server) clusterTags(ctx context.Context, orgId uint32, filter string) ([]string, error) {
	data := models.IndexTags{OrgId: orgId, Filter: filter}
	resps, err := s.peerQuerySpeculative(ctx, data, "clusterTags", "/index/tags")
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}

	tagSet := make(map[string]struct{})
	resp := models.IndexTagsResp{}
	for _, r := range resps {
		_, err = resp.UnmarshalMsg(r.buf)
		if err != nil {
			return nil, err
		}
		for _, tag := range resp.Tags {
			tagSet[tag] = struct{}{}
		}
	}

	// we want to make an empty list, because this results in the json response "[]" if it's empty
	// if we initialize "tags" with "var tags []string" the json response (if empty) is "nil" instead of "[]"
	tags := make([]string, 0, len(tagSet))
	for t := range tagSet {
		tags = append(tags, t)
	}

	return tags, nil
}

func (s *Server) graphiteAutoCompleteTags(ctx *middleware.Context, request models.GraphiteAutoCompleteTags) {
	if request.Limit == 0 {
		request.Limit = tagdbDefaultLimit
	}

	tags, err := s.clusterAutoCompleteTags(ctx.Req.Context(), ctx.OrgId, request.Prefix, request.Expr, request.Limit)
	if err != nil {
		response.Write(ctx, response.WrapErrorForTagDB(err))
		return
	}

	response.Write(ctx, response.NewJson(200, tags, ""))
}

func (s *Server) clusterAutoCompleteTags(ctx context.Context, orgId uint32, prefix string, expressions []string, limit uint) ([]string, error) {
	tagSet := make(map[string]struct{})

	data := models.IndexAutoCompleteTags{OrgId: orgId, Prefix: prefix, Expr: expressions, Limit: limit}
	responses, err := s.peerQuerySpeculative(ctx, data, "clusterAutoCompleteTags", "/index/tags/autoComplete/tags")
	if err != nil {
		return nil, err
	}

	resp := models.StringList{}
	for _, response := range responses {
		_, err = resp.UnmarshalMsg(response.buf)
		if err != nil {
			return nil, err
		}
		for _, tag := range resp {
			tagSet[tag] = struct{}{}
		}
	}

	tags := make([]string, 0, len(tagSet))
	for t := range tagSet {
		tags = append(tags, t)
	}

	sort.Strings(tags)
	if uint(len(tags)) > limit {
		tags = tags[:limit]
	}

	return tags, nil
}

func (s *Server) graphiteAutoCompleteTagValues(ctx *middleware.Context, request models.GraphiteAutoCompleteTagValues) {
	if request.Limit == 0 {
		request.Limit = tagdbDefaultLimit
	}

	resp, err := s.clusterAutoCompleteTagValues(ctx.Req.Context(), ctx.OrgId, request.Tag, request.Prefix, request.Expr, request.Limit)
	if err != nil {
		response.Write(ctx, response.WrapErrorForTagDB(err))
		return
	}

	response.Write(ctx, response.NewJson(200, resp, ""))
}

func (s *Server) clusterAutoCompleteTagValues(ctx context.Context, orgId uint32, tag, prefix string, expressions []string, limit uint) ([]string, error) {
	valSet := make(map[string]struct{})

	data := models.IndexAutoCompleteTagValues{OrgId: orgId, Tag: tag, Prefix: prefix, Expr: expressions, Limit: limit}
	responses, err := s.peerQuerySpeculative(ctx, data, "clusterAutoCompleteValues", "/index/tags/autoComplete/values")
	if err != nil {
		return nil, err
	}

	var resp models.StringList
	for _, response := range responses {
		_, err = resp.UnmarshalMsg(response.buf)
		if err != nil {
			return nil, err
		}
		for _, val := range resp {
			valSet[val] = struct{}{}
		}
	}

	vals := make([]string, 0, len(valSet))
	for t := range valSet {
		vals = append(vals, t)
	}

	sort.Strings(vals)
	if uint(len(vals)) > limit {
		vals = vals[:limit]
	}

	return vals, nil
}

func (s *Server) graphiteTagTerms(ctx *middleware.Context, request models.GraphiteTagTerms) {
	data := models.IndexTagTerms{OrgId: ctx.OrgId, Tags: request.Tags, Expr: request.Expr}
	responses, err := s.peerQuerySpeculative(ctx.Req.Context(), data, "graphiteTagTerms", "/index/tags/terms")
	if err != nil {
		response.Write(ctx, response.WrapErrorForTagDB(err))
		return
	}

	var allTerms models.GraphiteTagTermsResp
	allTerms.Terms = make(map[string]map[string]uint32)

	for _, peerResponse := range responses {
		var resp models.GraphiteTagTermsResp
		_, err = resp.UnmarshalMsg(peerResponse.buf)
		if err != nil {
			response.Write(ctx, response.WrapErrorForTagDB(err))
			return
		}
		allTerms.TotalSeries += resp.TotalSeries
		for tag, terms := range resp.Terms {
			if _, ok := allTerms.Terms[tag]; !ok {
				allTerms.Terms[tag] = make(map[string]uint32)
			}
			for val, count := range terms {
				allTerms.Terms[tag][val] += count
			}

		}
	}
	response.Write(ctx, response.NewJson(200, allTerms, ""))
}

func (s *Server) graphiteFunctions(ctx *middleware.Context) {
	ctx.Req.Request.Body = ctx.Body
	graphiteProxy.ServeHTTP(ctx.Resp, ctx.Req.Request)
}

func (s *Server) graphiteTagDelSeries(ctx *middleware.Context, request models.GraphiteTagDelSeries) {
	res := models.GraphiteTagDelSeriesResp{}

	// nothing to do on query nodes.
	if s.MetricIndex != nil {
		for _, path := range request.Paths {
			tags, err := tagquery.ParseTagsFromMetricName(path)
			if err != nil {
				response.Write(ctx, response.WrapErrorForTagDB(err))
				return
			}

			expressions := make(tagquery.Expressions, len(tags))
			builder := strings.Builder{}
			for i := range tags {
				tags[i].StringIntoWriter(&builder)

				expressions[i], err = tagquery.ParseExpression(builder.String())
				if err != nil {
					response.Write(ctx, response.WrapErrorForTagDB(err))
					return
				}
				builder.Reset()
			}

			query, err := tagquery.NewQuery(expressions, 0)
			if err != nil {
				response.Write(ctx, response.WrapErrorForTagDB(err))
				return
			}

			deleted := s.MetricIndex.DeleteTagged(ctx.OrgId, query)
			res.Count += len(deleted)
		}
	}

	data := models.IndexTagDelSeries{OrgId: ctx.OrgId, Paths: request.Paths}
	responses, errors := s.peerQuery(ctx.Req.Context(), data, "clusterTagDelSeries,", "/index/tags/delSeries")

	// if there are any errors, write one of them and return
	for _, err := range errors {
		response.Write(ctx, response.WrapErrorForTagDB(err))
		return
	}

	res.Peers = make(map[string]int, len(responses))
	peerResp := models.IndexTagDelSeriesResp{}
	for peer, resp := range responses {
		_, err := peerResp.UnmarshalMsg(resp.buf)
		if err != nil {
			response.Write(ctx, response.WrapErrorForTagDB(err))
			return
		}
		res.Peers[peer] = peerResp.Count
	}

	response.Write(ctx, response.NewJson(200, res, ""))
}

// showPlan attempts to create a Plan given a /render target query.
// If the Plan creation is successful it returns 200, JSON marshaling of Plan.
// Otherwise, it returns 400, error details.
// This is needed to determine if a query cannot be resolved in localOnly mode.
func (s *Server) showPlan(ctx *middleware.Context, request models.GraphiteRender) {
	// note: the model is already validated to assure at least one of them has len >0
	if len(request.Targets) == 0 {
		request.Targets = request.TargetsRails
	}

	now := time.Now()
	defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	defaultTo := uint32(now.Unix())
	fromUnix, toUnix, err := getFromTo(request.FromTo, now, defaultFrom, defaultTo)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	if fromUnix >= toUnix {
		response.Write(ctx, response.NewError(http.StatusBadRequest, InvalidTimeRangeErr.Error()))
		return
	}

	// render API is modeled after graphite, so from exclusive, to inclusive.
	// in MT, from is inclusive, to is exclusive (which is akin to slice syntax)
	// so we must adjust
	fromUnix += 1
	toUnix += 1

	exprs, err := expr.ParseMany(request.Targets)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	stable := request.Process == "stable"
	mdp := request.MaxDataPoints

	opts, err := optimizations.ApplyUserPrefs(request.Optimizations)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	plan, err := expr.NewPlan(exprs, fromUnix, toUnix, mdp, stable, opts)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	switch request.Format {
	case "json":
		response.Write(ctx, response.NewJson(200, plan, ""))
	default:
		response.Write(ctx, response.NewError(http.StatusBadRequest, "Unsupported response format requested: "+request.Format))
	}
}

func (s *Server) getMetaTagRecords(ctx *middleware.Context) {
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewJson(200, []tagquery.MetaTagRecord{}, ""))
		return
	}
	metaTagRecords := s.MetricIndex.MetaTagRecordList(ctx.OrgId)
	response.Write(ctx, response.NewJson(200, metaTagRecords, ""))
}

func (s *Server) metaTagRecordUpsert(ctx *middleware.Context, upsertRequest models.MetaTagRecordUpsert) {
	if s.MetricIndex == nil {
		response.Write(ctx, response.WrapError(fmt.Errorf("No metric index present")))
		return
	}

	record, err := tagquery.ParseMetaTagRecord(upsertRequest.MetaTags, upsertRequest.Expressions)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	err = s.MetricIndex.MetaTagRecordUpsert(ctx.OrgId, record)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	response.Write(ctx, response.NewJson(200, struct{ Status string }{Status: "OK"}, ""))
}

func (s *Server) metaTagRecordSwap(ctx *middleware.Context, swapRequest models.MetaTagRecordSwap) {
	if s.MetricIndex == nil {
		response.Write(ctx, response.WrapError(fmt.Errorf("No metric index present")))
		return
	}

	var err error
	metaTagRecords := make([]tagquery.MetaTagRecord, len(swapRequest.Records))
	for i, rawRecord := range swapRequest.Records {
		metaTagRecords[i], err = tagquery.ParseMetaTagRecord(rawRecord.MetaTags, rawRecord.Expressions)
		if err != nil {
			response.Write(ctx, response.Errorf(http.StatusBadRequest, "Error when parsing record %d: %s", i, err))
			return
		}
	}

	err = s.MetricIndex.MetaTagRecordSwap(ctx.OrgId, metaTagRecords)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	response.Write(ctx, response.NewJson(200, struct{ Status string }{Status: "OK"}, ""))
}
