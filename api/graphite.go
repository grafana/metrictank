package api

import (
	"errors"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/api/response"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/expr"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/metrictank/util"
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
)

type Series struct {
	Pattern string
	Series  []idx.Node
	Node    cluster.Node
}

func (s *Server) findSeries(orgId int, patterns []string, seenAfter int64) ([]Series, error) {
	peers := cluster.MembersForQuery()
	log.Debug("HTTP findSeries for %v across %d instances", patterns, len(peers))
	errors := make([]error, 0)
	series := make([]Series, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, peer := range peers {
		log.Debug("HTTP findSeries getting results from %s", peer.Name)
		wg.Add(1)
		if peer.IsLocal() {
			go func() {
				result, err := s.findSeriesLocal(orgId, patterns, seenAfter)
				mu.Lock()
				if err != nil {
					errors = append(errors, err)
				}
				series = append(series, result...)
				mu.Unlock()
				wg.Done()
			}()
		} else {
			go func(peer cluster.Node) {
				result, err := s.findSeriesRemote(orgId, patterns, seenAfter, peer)
				mu.Lock()
				if err != nil {
					errors = append(errors, err)
				}
				series = append(series, result...)
				mu.Unlock()
				wg.Done()
			}(peer)
		}
	}
	wg.Wait()
	var err error
	if len(errors) > 0 {
		err = errors[0]
	}

	return series, err
}

func (s *Server) findSeriesLocal(orgId int, patterns []string, seenAfter int64) ([]Series, error) {
	result := make([]Series, 0)
	for _, pattern := range patterns {
		nodes, err := s.MetricIndex.Find(orgId, pattern, seenAfter)
		if err != nil {
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

func (s *Server) findSeriesRemote(orgId int, patterns []string, seenAfter int64, peer cluster.Node) ([]Series, error) {
	log.Debug("HTTP Render querying %s/index/find for %d:%q", peer.Name, orgId, patterns)
	buf, err := peer.Post("/index/find", models.IndexFind{Patterns: patterns, OrgId: orgId, From: seenAfter})
	if err != nil {
		log.Error(4, "HTTP Render error querying %s/index/find: %q", peer.Name, err)
		return nil, err
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
	targets := request.Targets
	now := time.Now()

	from := request.From
	to := request.To
	if to == "" {
		to = request.Until
	}

	defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	defaultTo := uint32(now.Unix())

	fromUnix, err := dur.ParseTSpec(from, now, defaultFrom)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	toUnix, err := dur.ParseTSpec(to, now, defaultTo)
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

	exprs, err := expr.ParseMany(targets)
	if err != nil {
		ctx.Error(http.StatusBadRequest, err.Error())
		return
	}

	reqRenderTargetCount.Value(len(targets))

	if request.Process == "none" {
		ctx.Req.Request.Body = ctx.Body
		graphiteProxy.ServeHTTP(ctx.Resp, ctx.Req.Request)
		renderReqProxied.Inc()
		return
	}

	stable := request.Process == "stable"

	plan, err := expr.NewPlan(exprs, fromUnix, toUnix, request.MaxDataPoints, stable, nil)
	if err != nil {
		if fun, ok := err.(expr.ErrUnknownFunction); ok {
			if request.NoProxy {
				ctx.Error(http.StatusBadRequest, "localOnly requested, but the request cant be handled locally")
				return
			}
			ctx.Req.Request.Body = ctx.Body
			graphiteProxy.ServeHTTP(ctx.Resp, ctx.Req.Request)
			proxyStats.Miss(string(fun))
			renderReqProxied.Inc()
			return
		}
		ctx.Error(http.StatusBadRequest, err.Error())
		return
	}

	out, err := s.executePlan(ctx.OrgId, plan)
	if err != nil {
		ctx.Error(http.StatusBadRequest, err.Error())
		return
	}
	sort.Sort(models.SeriesByTarget(out))

	switch request.Format {
	case "msgp":
		response.Write(ctx, response.NewMsgp(200, models.SeriesByTarget(out)))
	case "pickle":
		response.Write(ctx, response.NewPickle(200, models.SeriesPickleFormat(out)))
	default:
		response.Write(ctx, response.NewFastJson(200, models.SeriesByTarget(out)))
	}
	plan.Clean()
}

func (s *Server) metricsFind(ctx *middleware.Context, request models.GraphiteFind) {
	nodes := make([]idx.Node, 0)
	series, err := s.findSeries(ctx.OrgId, []string{request.Query}, request.From)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
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
	case "pickle":
		response.Write(ctx, response.NewPickle(200, findPickle(nodes, request)))
	}
}

func (s *Server) listLocal(orgId int) []idx.Archive {
	return s.MetricIndex.List(orgId)
}

func (s *Server) listRemote(orgId int, peer cluster.Node) ([]idx.Archive, error) {
	log.Debug("HTTP IndexJson() querying %s/index/list for %d", peer.Name, orgId)
	buf, err := peer.Post("/index/list", models.IndexList{OrgId: orgId})
	if err != nil {
		log.Error(4, "HTTP IndexJson() error querying %s/index/list: %q", peer.Name, err)
		return nil, err
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
	peers := cluster.MembersForQuery()
	errors := make([]error, 0)
	series := make([]idx.Archive, 0)
	seenDefs := make(map[string]struct{})
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		if peer.IsLocal() {
			go func() {
				result := s.listLocal(ctx.OrgId)
				mu.Lock()
				for _, def := range result {
					if _, ok := seenDefs[def.Id]; !ok {
						series = append(series, def)
						seenDefs[def.Id] = struct{}{}
					}
				}
				mu.Unlock()
				wg.Done()
			}()
		} else {
			go func(peer cluster.Node) {
				result, err := s.listRemote(ctx.OrgId, peer)
				mu.Lock()
				if err != nil {
					errors = append(errors, err)
				}
				for _, def := range result {
					if _, ok := seenDefs[def.Id]; !ok {
						series = append(series, def)
						seenDefs[def.Id] = struct{}{}
					}
				}
				mu.Unlock()
				wg.Done()
			}(peer)
		}
	}
	wg.Wait()
	var err error
	if len(errors) > 0 {
		err = errors[0]
	}

	if err != nil {
		log.Error(3, "HTTP IndexJson() %s", err.Error())
		response.Write(ctx, response.WrapError(err))
		return
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

		i := strings.LastIndex(c.Path, ".")

		if i != -1 {
			c.Name = c.Path[i+1:]
		}
		result.Add(c)
	}

	return result
}

func findPickle(nodes []idx.Node, request models.GraphiteFind) models.SeriesPickle {
	result := make([]models.SeriesPickleItem, len(nodes))
	var intervals [][]int64
	if request.From != 0 && request.Until != 0 {
		intervals = [][]int64{{request.From, request.Until}}
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

	basepath := ""
	if i := strings.LastIndex(query, "."); i != -1 {
		basepath = query[:i+1]
	}

	for _, g := range nodes {
		name := string(g.Path)
		if i := strings.LastIndex(name, "."); i != -1 {
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
			ID:            basepath + name,
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
	errors := make([]error, 0)
	deleted := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, peer := range peers {
		log.Debug("HTTP metricsDelete getting results from %s", peer.Name)
		wg.Add(1)
		if peer.IsLocal() {
			go func() {
				result, err := s.metricsDeleteLocal(ctx.OrgId, req.Query)
				mu.Lock()
				if err != nil {
					// errors can be due to bad user input or corrupt index.
					if strings.Contains(err.Error(), "Index is corrupt") {
						errors = append(errors, response.NewError(http.StatusInternalServerError, err.Error()))
					} else {
						errors = append(errors, response.NewError(http.StatusBadRequest, err.Error()))
					}
				}
				deleted += result
				mu.Unlock()
				wg.Done()
			}()
		} else {
			go func(peer cluster.Node) {
				result, err := s.metricsDeleteRemote(ctx.OrgId, req.Query, peer)
				mu.Lock()
				if err != nil {
					errors = append(errors, err)
				}
				deleted += result
				mu.Unlock()
				wg.Done()
			}(peer)
		}
	}
	wg.Wait()
	var err error
	if len(errors) > 0 {
		response.Write(ctx, response.WrapError(err))
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

func (s *Server) metricsDeleteRemote(orgId int, query string, peer cluster.Node) (int, error) {
	log.Debug("HTTP metricDelete calling %s/index/delete for %d:%q", peer.Name, orgId, query)
	buf, err := peer.Post("/index/delete", models.IndexDelete{Query: query, OrgId: orgId})
	if err != nil {
		log.Error(4, "HTTP metricDelete error querying %s/index/delete: %q", peer.Name, err)
		return 0, err
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
func (s *Server) executePlan(orgId int, plan expr.Plan) ([]models.Series, error) {

	minFrom := uint32(math.MaxUint32)
	var maxTo uint32
	var reqs []models.Req

	// note that different patterns to query can have different from / to, so they require different index lookups
	// e.g. target=movingAvg(foo.*, "1h")&target=foo.*
	// note that in this case we fetch foo.* twice. can be optimized later
	for _, r := range plan.Reqs {
		series, err := s.findSeries(orgId, []string{r.Query}, int64(r.From))
		if err != nil {
			return nil, err
		}

		minFrom = util.Min(minFrom, r.From)
		maxTo = util.Max(maxTo, r.To)

		for _, s := range series {
			for _, metric := range s.Series {
				for _, archive := range metric.Defs {
					// set consolidator that will be used to normalize raw data before feeding into processing functions
					// not to be confused with runtime consolidation which happens after all processing.
					fn := mdata.Aggregations.Get(archive.AggId).AggregationMethod[0]
					consolidator := consolidation.Consolidator(fn) // we use the same number assignments so we can cast them
					reqs = append(reqs, models.NewReq(
						archive.Id, archive.Name, r.Query, r.From, r.To, plan.MaxDataPoints, uint32(archive.Interval), consolidator, s.Node, archive.SchemaId, archive.AggId))
				}
			}
		}
	}

	reqRenderSeriesCount.Value(len(reqs))
	if len(reqs) == 0 {
		return nil, nil
	}

	// note: if 1 series has a movingAvg that requires a long time range extension, it may push other reqs into another archive. can be optimized later
	reqs, err := alignRequests(uint32(time.Now().Unix()), minFrom, maxTo, reqs)
	if err != nil {
		log.Error(3, "HTTP Render alignReq error: %s", err)
		return nil, err
	}

	if LogLevel < 2 {
		for _, req := range reqs {
			log.Debug("HTTP Render %s - arch:%d archI:%d outI:%d aggN: %d from %s", req, req.Archive, req.ArchInterval, req.OutInterval, req.AggNum, req.Node.Name)
		}
	}

	out, err := s.getTargets(reqs)
	if err != nil {
		log.Error(3, "HTTP Render %s", err.Error())
		return nil, err
	}
	out = mergeSeries(out)

	// instead of waiting for all data to come in and then start processing everything, we could consider starting processing earlier, at the risk of doing needless work
	// if we need to cancel the request due to a fetch error

	data := make(map[expr.Req][]models.Series)
	for _, serie := range out {
		q := expr.Req{
			serie.QueryPatt,
			serie.QueryFrom,
			serie.QueryTo,
		}
		data[q] = append(data[q], serie)
	}

	return plan.Run(data)
}
