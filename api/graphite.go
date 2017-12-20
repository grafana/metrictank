package api

import (
	"context"
	"errors"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	macaron "gopkg.in/macaron.v1"

	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/expr"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/tracing"
	"github.com/grafana/metrictank/util"
	opentracing "github.com/opentracing/opentracing-go"
	tags "github.com/opentracing/opentracing-go/ext"
	"github.com/raintank/dur"
	"github.com/raintank/worldping-api/pkg/log"
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

	// metric api.request.render.series is the number of targets a /render request is handling.
	reqRenderTargetCount = stats.NewMeter32("api.request.render.targets", false)

	// metric plan.run is the time spent running the plan for a request (function processing of all targets and runtime consolidation)
	planRunDuration = stats.NewLatencyHistogram15s32("plan.run")
)

type Series struct {
	Pattern string // pattern used for index lookup. typically user input like foo.{b,a}r.*
	Series  []idx.Node
	Node    cluster.Node
}

func (s *Server) findSeries(ctx context.Context, orgId int, patterns []string, seenAfter int64) ([]Series, error) {
	peers, err := cluster.MembersForQuery()
	if err != nil {
		log.Error(3, "HTTP findSeries unable to get peers, %s", err)
		return nil, err
	}
	log.Debug("HTTP findSeries for %v across %d instances", patterns, len(peers))
	var wg sync.WaitGroup

	responses := make(chan struct {
		series []Series
		err    error
	}, 1)
	findCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, peer := range peers {
		log.Debug("HTTP findSeries getting results from %s", peer.Name)
		wg.Add(1)
		if peer.IsLocal() {
			go func() {
				result, err := s.findSeriesLocal(findCtx, orgId, patterns, seenAfter)
				if err != nil {
					// cancel requests on all other peers.
					cancel()
				}
				responses <- struct {
					series []Series
					err    error
				}{result, err}
				wg.Done()
			}()
		} else {
			go func(peer cluster.Node) {
				result, err := s.findSeriesRemote(findCtx, orgId, patterns, seenAfter, peer)
				if err != nil {
					// cancel requests on all other peers.
					cancel()
				}
				responses <- struct {
					series []Series
					err    error
				}{result, err}
				wg.Done()
			}(peer)
		}
	}

	// wait for all findSeries goroutines to end, then close our responses channel
	go func() {
		wg.Wait()
		close(responses)
	}()

	series := make([]Series, 0)
	for resp := range responses {
		if resp.err != nil {
			return nil, err
		}
		series = append(series, resp.series...)
	}

	return series, nil
}

func (s *Server) findSeriesLocal(ctx context.Context, orgId int, patterns []string, seenAfter int64) ([]Series, error) {
	result := make([]Series, 0)
	for _, pattern := range patterns {
		select {
		case <-ctx.Done():
			//request canceled
			return nil, nil
		default:
		}
		_, span := tracing.NewSpan(ctx, s.Tracer, "findSeriesLocal")
		span.SetTag("org", orgId)
		span.SetTag("pattern", pattern)
		defer span.Finish()
		nodes, err := s.MetricIndex.Find(orgId, pattern, seenAfter)
		if err != nil {
			tags.Error.Set(span, true)
			return nil, response.NewError(http.StatusBadRequest, err.Error())
		}
		result = append(result, Series{
			Pattern: pattern,
			Node:    cluster.Manager.ThisNode(),
			Series:  nodes,
		})
		log.Debug("HTTP findSeries %d matches for %s found locally", len(nodes), pattern)
	}
	return result, nil
}

func (s *Server) findSeriesRemote(ctx context.Context, orgId int, patterns []string, seenAfter int64, peer cluster.Node) ([]Series, error) {
	log.Debug("HTTP Render querying %s/index/find for %d:%q", peer.Name, orgId, patterns)
	data := models.IndexFind{
		Patterns: patterns,
		OrgId:    orgId,
		From:     seenAfter,
	}
	buf, err := peer.Post(ctx, "findSeriesRemote", "/index/find", data)
	if err != nil {
		log.Error(4, "HTTP Render error querying %s/index/find: %q", peer.Name, err)
		return nil, err
	}
	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}
	resp := models.NewIndexFindResp()
	_, err = resp.UnmarshalMsg(buf)
	if err != nil {
		log.Error(4, "HTTP Find() error unmarshaling body from %s/index/find: %q", peer.Name, err)
		return nil, err
	}
	result := make([]Series, 0)
	for pattern, nodes := range resp.Nodes {
		result = append(result, Series{
			Pattern: pattern,
			Node:    peer,
			Series:  nodes,
		})
		log.Debug("HTTP findSeries %d matches for %s found on %s", len(nodes), pattern, peer.Name)
	}
	return result, nil
}

func (s *Server) renderMetrics(ctx *middleware.Context, request models.GraphiteRender) {
	span := opentracing.SpanFromContext(ctx.Req.Context())

	// note: the model is already validated to assure at least one of them has len >0
	if len(request.Targets) == 0 {
		request.Targets = request.TargetsRails
	}

	span.SetTag("from", request.FromTo.From)
	span.SetTag("until", request.FromTo.Until)
	span.SetTag("to", request.FromTo.To)
	span.SetTag("tz", request.FromTo.Tz)
	span.SetTag("mdp", request.MaxDataPoints)
	span.SetTag("targets", request.Targets)
	span.SetTag("format", request.Format)
	span.SetTag("noproxy", request.NoProxy)
	span.SetTag("process", request.Process)

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

	span.SetTag("fromUnix", fromUnix)
	span.SetTag("toUnix", toUnix)
	span.SetTag("span", toUnix-fromUnix)

	// render API is modeled after graphite, so from exclusive, to inclusive.
	// in MT, from is inclusive, to is exclusive (which is akin to slice syntax)
	// so we must adjust
	fromUnix += 1
	toUnix += 1

	exprs, err := expr.ParseMany(request.Targets)
	if err != nil {
		ctx.Error(http.StatusBadRequest, err.Error())
		return
	}

	reqRenderTargetCount.Value(len(request.Targets))

	if request.Process == "none" {
		ctx.Req.Request.Body = ctx.Body
		graphiteProxy.ServeHTTP(ctx.Resp, ctx.Req.Request)
		renderReqProxied.Inc()
		return
	}

	stable := request.Process == "stable"
	mdp := request.MaxDataPoints
	if request.NoProxy {
		// if this request is coming from graphite, we should not do runtime consolidation
		// as graphite needs high-res data to perform its processing.
		mdp = 0
	}
	plan, err := expr.NewPlan(exprs, fromUnix, toUnix, mdp, stable, nil)
	if err != nil {
		if fun, ok := err.(expr.ErrUnknownFunction); ok {
			if request.NoProxy {
				ctx.Error(http.StatusBadRequest, "localOnly requested, but the request cant be handled locally")
				return
			}
			newctx, span := tracing.NewSpan(ctx.Req.Context(), s.Tracer, "graphiteproxy")
			tags.SpanKindRPCClient.Set(span)
			tags.PeerService.Set(span, "graphite")
			ctx.Req = macaron.Request{ctx.Req.WithContext(newctx)}
			ctx.Req.Request.Body = ctx.Body
			graphiteProxy.ServeHTTP(ctx.Resp, ctx.Req.Request)
			if span != nil {
				span.Finish()
			}
			proxyStats.Miss(string(fun))
			renderReqProxied.Inc()
			return
		}
		ctx.Error(http.StatusBadRequest, err.Error())
		return
	}

	newctx, span := tracing.NewSpan(ctx.Req.Context(), s.Tracer, "executePlan")
	defer span.Finish()
	ctx.Req = macaron.Request{ctx.Req.WithContext(newctx)}
	out, err := s.executePlan(ctx.Req.Context(), ctx.OrgId, plan)
	if err != nil {
		tracing.Failure(span)
		tracing.Error(span, err)
		response.Write(ctx, response.WrapError(err))
		return
	}

	// check to see if the request has been canceled, if so abort now.
	select {
	case <-newctx.Done():
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
		response.Write(ctx, response.NewFastJson(200, models.SeriesByTarget(out)))
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

func (s *Server) listLocal(orgId int) []idx.Archive {
	return s.MetricIndex.List(orgId)
}

func (s *Server) listRemote(ctx context.Context, orgId int, peer cluster.Node) ([]idx.Archive, error) {
	log.Debug("HTTP IndexJson() querying %s/index/list for %d", peer.Name, orgId)
	buf, err := peer.Post(ctx, "listRemote", "/index/list", models.IndexList{OrgId: orgId})
	if err != nil {
		log.Error(4, "HTTP IndexJson() error querying %s/index/list: %q", peer.Name, err)
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
			log.Error(3, "HTTP IndexJson() error unmarshaling body from %s/index/list: %q", peer.Name, err)
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
	seenDefs := make(map[string]struct{})
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
	tree := models.NewSeriesTree()
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
	return *tree
}

func (s *Server) metricsDelete(ctx *middleware.Context, req models.MetricsDelete) {
	peers := cluster.Manager.MemberList()
	peers = append(peers, cluster.Manager.ThisNode())
	log.Debug("HTTP metricsDelete for %v across %d instances", req.Query, len(peers))

	reqCtx, cancel := context.WithCancel(ctx.Req.Context())
	defer cancel()
	deleted := 0
	responses := make(chan struct {
		deleted int
		err     error
	}, len(peers))
	var wg sync.WaitGroup
	for _, peer := range peers {
		log.Debug("HTTP metricsDelete getting results from %s", peer.Name)
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

func (s *Server) metricsDeleteLocal(orgId int, query string) (int, error) {
	defs, err := s.MetricIndex.Delete(orgId, query)
	return len(defs), err
}

func (s *Server) metricsDeleteRemote(ctx context.Context, orgId int, query string, peer cluster.Node) (int, error) {
	log.Debug("HTTP metricDelete calling %s/index/delete for %d:%q", peer.Name, orgId, query)

	body := models.IndexDelete{
		Query: query,
		OrgId: orgId,
	}
	buf, err := peer.Post(ctx, "metricsDeleteRemote", "/index/delete", body)
	if err != nil {
		log.Error(4, "HTTP metricDelete error querying %s/index/delete: %q", peer.Name, err)
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
		log.Error(4, "HTTP metricDelete error unmarshaling body from %s/index/delete: %q", peer.Name, err)
		return 0, err
	}

	return resp.DeletedDefs, nil
}

// executePlan looks up the needed data, retrieves it, and then invokes the processing
// note if you do something like sum(foo.*) and all of those metrics happen to be on another node,
// we will collect all the indidividual series from the peer, and then sum here. that could be optimized
func (s *Server) executePlan(ctx context.Context, orgId int, plan expr.Plan) ([]models.Series, error) {

	minFrom := uint32(math.MaxUint32)
	var maxTo uint32
	var reqs []models.Req

	// note that different patterns to query can have different from / to, so they require different index lookups
	// e.g. target=movingAvg(foo.*, "1h")&target=foo.*
	// note that in this case we fetch foo.* twice. can be optimized later
	for _, r := range plan.Reqs {
		select {
		case <-ctx.Done():
			//request canceled
			return nil, nil
		default:
		}
		var err error
		var series []Series
		const SeriesByTagIdent = "seriesByTag("
		if strings.HasPrefix(r.Query, SeriesByTagIdent) {
			startPos := len(SeriesByTagIdent)
			endPos := strings.LastIndex(r.Query, ")")
			exprs := strings.Split(r.Query[startPos:endPos], ",")
			for i, e := range exprs {
				exprs[i] = strings.Trim(e, " '\"")
			}
			series, err = s.clusterFindByTag(ctx, orgId, exprs, int64(r.From))
		} else {
			series, err = s.findSeries(ctx, orgId, []string{r.Query}, int64(r.From))
		}
		if err != nil {
			return nil, err
		}

		minFrom = util.Min(minFrom, r.From)
		maxTo = util.Max(maxTo, r.To)

		for _, s := range series {
			for _, metric := range s.Series {
				for _, archive := range metric.Defs {
					cons := r.Cons
					consReq := r.Cons
					if consReq == 0 {
						// unless the user overrode the consolidation to use via a consolidateBy
						// we will use the primary method dictated by the storage-aggregations rules
						// note:
						// * we can't just let the expr library take care of normalization, as we may have to fetch targets
						//   from cluster peers; it's more efficient to have them normalize the data at the source.
						// * a pattern may expand to multiple series, each of which can have their own aggregation method.
						fn := mdata.Aggregations.Get(archive.AggId).AggregationMethod[0]
						cons = consolidation.Consolidator(fn) // we use the same number assignments so we can cast them
					}

					newReq := models.NewReq(
						archive.Id, archive.NameWithTags(), r.Query, r.From, r.To, plan.MaxDataPoints, uint32(archive.Interval), cons, consReq, s.Node, archive.SchemaId, archive.AggId)
					reqs = append(reqs, newReq)
				}
			}
		}
	}

	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}

	reqRenderSeriesCount.Value(len(reqs))
	if len(reqs) == 0 {
		return nil, nil
	}

	// note: if 1 series has a movingAvg that requires a long time range extension, it may push other reqs into another archive. can be optimized later
	reqs, pointsFetch, pointsReturn, err := alignRequests(uint32(time.Now().Unix()), minFrom, maxTo, reqs)
	if err != nil {
		log.Error(3, "HTTP Render alignReq error: %s", err)
		return nil, err
	}
	span := opentracing.SpanFromContext(ctx)
	span.SetTag("points_fetch", pointsFetch)
	span.SetTag("points_return", pointsReturn)

	if LogLevel < 2 {
		for _, req := range reqs {
			log.Debug("HTTP Render %s - arch:%d archI:%d outI:%d aggN: %d from %s", req, req.Archive, req.ArchInterval, req.OutInterval, req.AggNum, req.Node.Name)
		}
	}

	out, err := s.getTargets(ctx, reqs)
	if err != nil {
		log.Error(3, "HTTP Render %s", err.Error())
		return nil, err
	}

	out = mergeSeries(out)

	// instead of waiting for all data to come in and then start processing everything, we could consider starting processing earlier, at the risk of doing needless work
	// if we need to cancel the request due to a fetch error

	data := make(map[expr.Req][]models.Series)
	for _, serie := range out {
		q := expr.NewReq(serie.QueryPatt, serie.QueryFrom, serie.QueryTo, serie.QueryCons)
		data[q] = append(data[q], serie)
	}

	preRun := time.Now()
	out, err = plan.Run(data)
	planRunDuration.Value(time.Since(preRun))
	return out, err
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
	tagValues, err := s.clusterTagDetails(reqCtx, ctx.OrgId, tag, request.Filter, request.From)
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

func (s *Server) clusterTagDetails(ctx context.Context, orgId int, tag, filter string, from int64) (map[string]uint64, error) {
	result, err := s.MetricIndex.TagDetails(orgId, tag, filter, from)
	if err != nil {
		return nil, err
	}
	if result == nil {
		result = make(map[string]uint64)
	}
	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}

	data := models.IndexTagDetails{OrgId: orgId, Tag: tag, Filter: filter, From: from}
	resps, err := s.peerQuery(ctx, data, "clusterTagDetails", "/index/tag_details")
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
	series, err := s.clusterFindByTag(reqCtx, ctx.OrgId, request.Expr, request.From)
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
	seriesNames := make([]string, 0, len(series))
	for _, serie := range series {
		seriesNames = append(seriesNames, serie.Pattern)
	}
	response.Write(ctx, response.NewJson(200, seriesNames, ""))
}

func (s *Server) clusterFindByTag(ctx context.Context, orgId int, expressions []string, from int64) ([]Series, error) {
	seriesSet := make(map[string]Series)

	result, err := s.MetricIndex.FindByTag(orgId, expressions, from)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}

	for _, series := range result {
		seriesSet[series.Path] = Series{
			Pattern: series.Path,
			Node:    cluster.Manager.ThisNode(),
			Series:  []idx.Node{series},
		}
	}

	data := models.IndexFindByTag{OrgId: orgId, Expr: expressions, From: from}
	resps, err := s.peerQuery(ctx, data, "clusterFindByTag", "/index/find_by_tag")
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}

	for _, r := range resps {
		resp := models.IndexFindByTagResp{}
		_, err = resp.UnmarshalMsg(r.buf)
		if err != nil {
			return nil, err
		}
		for _, series := range resp.Metrics {
			seriesSet[series.Path] = Series{
				Pattern: series.Path,
				Node:    r.peer,
				Series:  []idx.Node{series},
			}
		}
	}

	series := make([]Series, 0, len(seriesSet))
	for _, s := range seriesSet {
		series = append(series, s)
	}

	return series, nil
}

func (s *Server) graphiteTags(ctx *middleware.Context, request models.GraphiteTags) {
	reqCtx := ctx.Req.Context()
	tags, err := s.clusterTags(reqCtx, ctx.OrgId, request.Filter, request.From)
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

	var resp models.GraphiteTagsResp
	for _, tag := range tags {
		resp = append(resp, models.GraphiteTagResp{Tag: tag})
	}
	response.Write(ctx, response.NewJson(200, resp, ""))
}

func (s *Server) clusterTags(ctx context.Context, orgId int, filter string, from int64) ([]string, error) {
	result, err := s.MetricIndex.Tags(orgId, filter, from)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}

	tagSet := make(map[string]struct{}, len(result))
	for _, tag := range result {
		tagSet[tag] = struct{}{}
	}

	data := models.IndexTags{OrgId: orgId, Filter: filter, From: from}
	resps, err := s.peerQuery(ctx, data, "clusterTags", "/index/tags")
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}

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

	tags, err := s.clusterAutoCompleteTags(ctx.Req.Context(), ctx.OrgId, request.Prefix, request.Expr, request.From, request.Limit)
	if err != nil {
		response.Write(ctx, response.WrapErrorForTagDB(err))
		return
	}

	response.Write(ctx, response.NewJson(200, tags, ""))
}

func (s *Server) clusterAutoCompleteTags(ctx context.Context, orgId int, prefix string, expressions []string, from int64, limit uint) ([]string, error) {
	result, err := s.MetricIndex.FindTags(orgId, prefix, expressions, from, limit)
	if err != nil {
		return nil, err
	}

	tagSet := make(map[string]struct{}, len(result))
	for _, tag := range result {
		tagSet[tag] = struct{}{}
	}

	data := models.IndexAutoCompleteTags{OrgId: orgId, Prefix: prefix, Expr: expressions, From: from, Limit: limit}
	responses, err := s.peerQuery(ctx, data, "clusterAutoCompleteTags", "/index/tags/autoComplete/tags")
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

	resp, err := s.clusterAutoCompleteTagValues(ctx.Req.Context(), ctx.OrgId, request.Tag, request.Prefix, request.Expr, request.From, request.Limit)
	if err != nil {
		response.Write(ctx, response.WrapErrorForTagDB(err))
		return
	}

	response.Write(ctx, response.NewJson(200, resp, ""))
}

func (s *Server) clusterAutoCompleteTagValues(ctx context.Context, orgId int, tag, prefix string, expressions []string, from int64, limit uint) ([]string, error) {
	result, err := s.MetricIndex.FindTagValues(orgId, tag, prefix, expressions, from, limit)
	if err != nil {
		return nil, err
	}

	valSet := make(map[string]struct{}, len(result))
	for _, val := range result {
		valSet[val] = struct{}{}
	}

	data := models.IndexAutoCompleteTagValues{OrgId: orgId, Tag: tag, Prefix: prefix, Expr: expressions, From: from, Limit: limit}
	responses, err := s.peerQuery(ctx, data, "clusterAutoCompleteValues", "/index/tags/autoComplete/values")
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
