package api

import (
	"errors"
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
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

var MissingOrgHeaderErr = errors.New("orgId not set in headers")
var MissingQueryErr = errors.New("missing query param")
var InvalidFormatErr = errors.New("invalid format specified")
var MaxPointsPerReqErr = errors.New("request exceeds max-points-per-req limit. Reduce the number of series and or maxDataPoints requested or ask your admin to increase the limit.")
var MaxDaysPerReqErr = errors.New("request exceeds max-days-per-req limit. Reduce the number of series and or time range requested or ask your admin to increase the limit.")
var InvalidTimeRangeErr = errors.New("invalid time range requested")

type Series struct {
	Pattern string
	Series  []idx.Node
	Node    cluster.Node
}

func parseTarget(target string) (string, string, error) {
	var consolidateBy string
	// yes, i am aware of the arguably grossness of the below.
	// however, it is solid based on the documented allowed input format.
	// once we need to support several functions, we can implement
	// a proper expression parser
	if strings.HasPrefix(target, "consolidateBy(") {
		var q1, q2 int
		t := target
		if t[len(t)-2:] == "')" && (strings.Contains(t, ",'") || strings.Contains(t, ", '")) && strings.Count(t, "'") == 2 {
			q1 = strings.Index(t, "'")
			q2 = strings.LastIndex(t, "'")
		} else if t[len(t)-2:] == "\")" && (strings.Contains(t, ",\"") || strings.Contains(t, ", \"")) && strings.Count(t, "\"") == 2 {
			q1 = strings.Index(t, "\"")
			q2 = strings.LastIndex(t, "\"")
		} else {
			return "", "", response.NewError(http.StatusBadRequest, "target parse error")
		}
		consolidateBy = t[q1+1 : q2]
		err := consolidation.Validate(consolidateBy)
		if err != nil {
			return "", "", err
		}
		target = t[strings.Index(t, "(")+1 : strings.LastIndex(t, ",")]
	}
	return target, consolidateBy, nil
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
	// metricDefs only get updated periodically, so we add a 1day (86400seconds) buffer when
	// filtering by our From timestamp.  This should be moved to a configuration option,
	// but that will require significant refactoring to expose the updateInterval used
	// in the MetricIdx.
	if seenAfter != 0 {
		seenAfter -= 86400
	}
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
	buf, err := peer.Post("/index/find", models.IndexFind{Patterns: patterns, OrgId: orgId})
	if err != nil {
		log.Error(4, "HTTP Render error querying %s/index/find: %q", peer.Name, err)
		return nil, err
	}
	resp := models.NewIndexFindResp()
	buf, err = resp.UnmarshalMsg(buf)
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
	pre := time.Now()

	targets := request.Targets
	if maxPointsPerReq != 0 && len(targets)*int(request.MaxDataPoints) > maxPointsPerReq {
		response.Write(ctx, response.NewError(http.StatusForbidden, MaxPointsPerReqErr.Error()))
		return
	}

	now := time.Now()

	from := request.From
	to := request.To
	if to == "" {
		to = request.Until
	}

	defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	defaultTo := uint32(now.Add(time.Duration(1) * time.Second).Unix())

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

	// in MT, both the external and internal api, from is inclusive, to is exclusive
	// in graphite, from is exclusive and to inclusive
	// so in this case, adjust for internal api.
	fromUnix += 1
	toUnix += 1

	if fromUnix >= toUnix {
		response.Write(ctx, response.NewError(http.StatusBadRequest, InvalidTimeRangeErr.Error()))
		return
	}
	if maxDaysPerReq != 0 && len(targets)*int(toUnix-fromUnix) > maxDaysPerReq*(3600*24) {
		response.Write(ctx, response.NewError(http.StatusForbidden, MaxDaysPerReqErr.Error()))
		return
	}

	reqs := make([]models.Req, 0)

	// consolidatorForPattern[<pattern>]<consolidateBy>
	consolidatorForPattern := make(map[string]string)
	patterns := make([]string, 0)
	type locatedDef struct {
		def  schema.MetricDefinition
		node cluster.Node
	}

	//locatedDefs[<pattern>][<def.id>]locatedDef
	locatedDefs := make(map[string]map[string]locatedDef)
	//targetForPattern[<pattern>]<target>
	targetForPattern := make(map[string]string)
	for _, target := range targets {
		pattern, consolidateBy, err := parseTarget(target)
		if err != nil {
			ctx.Error(http.StatusBadRequest, err.Error())
			return
		}
		consolidatorForPattern[pattern] = consolidateBy
		patterns = append(patterns, pattern)
		targetForPattern[pattern] = target
		locatedDefs[pattern] = make(map[string]locatedDef)

	}

	series, err := s.findSeries(ctx.OrgId, patterns, int64(fromUnix))
	if err != nil {
		response.Write(ctx, response.WrapError(err))
	}

	for _, s := range series {
		for _, metric := range s.Series {
			if !metric.Leaf {
				continue
			}
			for _, def := range metric.Defs {
				locatedDefs[s.Pattern][def.Id] = locatedDef{def, s.Node}
			}
		}
	}

	for pattern, ldefs := range locatedDefs {
		for _, locdef := range ldefs {
			def := locdef.def
			consolidator, err := consolidation.GetConsolidator(&def, consolidatorForPattern[pattern])
			if err != nil {
				response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
				return
			}
			// target is like foo.bar or foo.* or consolidateBy(foo.*,'sum')
			// pattern is like foo.bar or foo.*
			// def.Name is like foo.concretebar
			// so we want target to contain the concrete graphite name, potentially wrapped with consolidateBy().
			target := strings.Replace(targetForPattern[pattern], pattern, def.Name, -1)
			reqs = append(reqs, models.NewReq(def.Id, target, fromUnix, toUnix, request.MaxDataPoints, uint32(def.Interval), consolidator, locdef.node))
		}
	}

	if len(reqs) == 0 {
		response.Write(ctx, response.NewJson(200, []string{}, ""))
		return
	}

	if (toUnix - fromUnix) >= logMinDur {
		log.Info("HTTP Render: INCOMING REQ %q from: %q, to: %q targets: %q, maxDataPoints: %d",
			ctx.Req.Method, from, to, request.Targets, request.MaxDataPoints)
	}

	reqs, err = alignRequests(reqs, s.MemoryStore.AggSettings())
	if err != nil {
		log.Error(3, "HTTP Render alignReq error: %s", err)
		response.Write(ctx, response.WrapError(err))
		return
	}

	if LogLevel < 2 {
		for _, req := range reqs {
			log.Debug("HTTP Render %s - arch:%d archI:%d outI:%d aggN: %d from %s", req, req.Archive, req.ArchInterval, req.OutInterval, req.AggNum, req.Node.Name)
		}
	}

	out, err := s.getTargets(reqs)
	if err != nil {
		log.Error(3, "HTTP Render %s", err.Error())
		response.Write(ctx, response.WrapError(err))
		return
	}

	merged := mergeSeries(out)
	sort.Sort(models.SeriesByTarget(merged))
	defer func() {
		for _, serie := range out {
			pointSlicePool.Put(serie.Datapoints[:0])
		}
	}()

	reqHandleDuration.Value(time.Now().Sub(pre))
	response.Write(ctx, response.NewFastJson(200, models.SeriesByTarget(merged)))
}

func (s *Server) metricsFind(ctx *middleware.Context, request models.GraphiteFind) {
	// metricDefs only get updated periodically (when using CassandraIdx), so we add a 1day (86400seconds) buffer when
	// filtering by our From timestamp.  This should be moved to a configuration option,
	// but that will require significant refactoring to expose the updateInterval used
	// in the MetricIdx.  So this will have to do for now.
	if request.From != 0 {
		request.From -= 86400
	}
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

	var b interface{}
	switch request.Format {
	case "", "treejson", "json":
		b, err = findTreejson(request.Query, nodes)
	case "completer":
		b, err = findCompleter(nodes)
	}

	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}
	response.Write(ctx, response.NewJson(200, b, request.Jsonp))
}

func (s *Server) listLocal(orgId int) []schema.MetricDefinition {
	return s.MetricIndex.List(orgId)
}

func (s *Server) listRemote(orgId int, peer cluster.Node) ([]schema.MetricDefinition, error) {
	log.Debug("HTTP IndexJson() querying %s/index/list for %d", peer.Name, orgId)
	buf, err := peer.Post("/index/list", models.IndexList{OrgId: orgId})
	if err != nil {
		log.Error(4, "HTTP IndexJson() error querying %s/index/list: %q", peer.Name, err)
		return nil, err
	}
	result := make([]schema.MetricDefinition, 0)
	for len(buf) != 0 {
		var def schema.MetricDefinition
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
	series := make([]schema.MetricDefinition, 0)
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

func findCompleter(nodes []idx.Node) (models.SeriesCompleter, error) {
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

	return result, nil
}

var treejsonContext = make(map[string]int)

func findTreejson(query string, nodes []idx.Node) (models.SeriesTree, error) {
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
	return *tree, nil
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
	buf, err = resp.UnmarshalMsg(buf)
	if err != nil {
		log.Error(4, "HTTP metricDelete error unmarshaling body from %s/index/delete: %q", peer.Name, err)
		return 0, err
	}

	return resp.DeletedDefs, nil
}
