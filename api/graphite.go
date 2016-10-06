package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/api/rbody"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/util"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

var MissingOrgHeaderErr = errors.New("orgId not set in headers")
var MissingQueryErr = errors.New("missing query param")
var InvalidFromatErr = errors.New("invalid format specified")
var MaxPointsPerReqErr = errors.New("request would execed maxPointsPerReq setting")
var MaxDaysPerReqErr = errors.New("request would execed maxDaysPerReq setting")
var InvalidTimeRangeErr = errors.New("invalid time range requested")

func (s *Server) renderMetrics(ctx *middleware.Context, request models.GraphiteRender) {
	if ctx.OrgId == 0 {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingOrgHeaderErr))
		return
	}
	pre := time.Now()
	maxDataPoints := uint32(800)
	if request.MaxDataPoints != 0 {
		maxDataPoints = request.MaxDataPoints
	}

	targets := request.Targets
	if maxPointsPerReq != 0 && len(targets)*int(maxDataPoints) > maxPointsPerReq {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MaxPointsPerReqErr))
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
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, err))
		return
	}

	toUnix, err := dur.ParseTSpec(to, now, defaultTo)
	if err != nil {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, err))
		return
	}

	// in MT, both the external and internal api, from is inclusive, to is exclusive
	// in graphite, from is exclusive and to inclusive
	// so in this case, adjust for internal api.
	fromUnix += 1
	toUnix += 1

	if fromUnix >= toUnix {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, InvalidTimeRangeErr))
		return
	}
	if maxDaysPerReq != 0 && len(targets)*int(toUnix-fromUnix) > maxDaysPerReq*(3600*24) {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MaxDaysPerReqErr))
		return
	}

	reqs := make([]models.Req, 0)
	parsedTargets := make(map[string]string)
	patterns := make([]string, 0)
	type locatedDef struct {
		def  schema.MetricDefinition
		node *cluster.Node
	}

	locatedDefs := make(map[string]map[string]locatedDef)
	queryForTarget := make(map[string]string)
	for _, query := range targets {
		target, consolidateBy, err := parseTarget(query)
		if err != nil {
			ctx.Error(http.StatusBadRequest, err.Error())
			return
		}
		parsedTargets[target] = consolidateBy
		patterns = append(patterns, target)
		queryForTarget[target] = query
		locatedDefs[target] = make(map[string]locatedDef)

		// metricDefs only get updated periodically, so we add a 1day (86400seconds) buffer when
		// filtering by our From timestamp.  This should be moved to a configuration option,
		// but that will require significant refactoring to expose the updateInterval used
		// in the MetricIdx.
		seenAfter := int64(fromUnix)
		if seenAfter != 0 {
			seenAfter -= 86400
		}
		nodes, err := s.MetricIndex.Find(ctx.OrgId, target, seenAfter)
		if err != nil {
			rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, err))
			return
		}
		for _, node := range nodes {
			for _, def := range node.Defs {
				locatedDefs[target][def.Id] = locatedDef{def, cluster.ThisNode}
			}
		}
	}
	var defsLock sync.Mutex
	peers := s.ClusterMgr.PeersForQuery()
	respCh := make(chan error, len(peers))
	var wg sync.WaitGroup
	for _, inst := range peers {
		wg.Add(1)
		go func(inst *cluster.Node) {
			defer wg.Done()
			if LogLevel < 2 {
				log.Debug("HTTP Render querying %s/internal/index/find for %d:%q", inst.GetName(), ctx.OrgId, patterns)
			}

			buf, err := inst.Post("/internal/index/find", models.IndexFind{Patterns: patterns, OrgId: ctx.OrgId})
			if err != nil {
				log.Error(4, "HTTP Render error querying %s/internal/index/find: %q", inst.GetName(), err)
				respCh <- err
				return
			}
			resp := models.NewIndexFindResp()
			count := 0
			for len(buf) != 0 {
				buf, err = resp.UnmarshalMsg(buf)
				if err != nil {
					log.Error(4, "HTTP Find() error unmarshaling body from %s/internal/index/find: %q", inst.GetName(), err)
					rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
					return
				}
				// different nodes may have overlapping data in their index.
				// maybe because they loaded the entire index from a persistent store,
				// or they used to receive a certain shard. or because they host metrics under branches
				// that other nodes also host metrics under
				// it may even happen that a node has a leaf that for another node is a branch, if the
				// org has been sending improper data.  in this case there's no elegant way to nicely handle this
				// so we'll just ignore one of them like we ignore other paths we've already seen.
				defsLock.Lock()
				for pattern, nodes := range resp.Nodes {
					for _, n := range nodes {
						count++
						for _, def := range n.Defs {
							cur, ok := locatedDefs[pattern][def.Id]
							if ok && cur.def.LastUpdate >= def.LastUpdate {
								continue
							}
							locatedDefs[pattern][def.Id] = locatedDef{def, inst}
						}
					}
				}
				defsLock.Unlock()
			}
			if LogLevel < 2 {
				log.Debug("HTTP Render response from %s/internal/index/find for %d:%q had %d nodes", inst.GetName(), ctx.OrgId, patterns, count)
			}
			respCh <- nil
		}(inst)
	}
	wg.Wait()
	close(respCh)
	errCount := 0
	for err := range respCh {
		if err != nil {
			errCount++
		}
	}
	// should we return an error here, or just return the partial results we have?
	if errCount > 0 {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, fmt.Errorf("%d of %d peer lookups failed", errCount, len(peers))))
		return
	}
	for target, ldefs := range locatedDefs {
		for _, locdef := range ldefs {
			def := locdef.def
			consolidator, err := consolidation.GetConsolidator(&def, parsedTargets[target])
			if err != nil {
				rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, err))
				return
			}
			// query is like foo.bar or foo.* or consolidateBy(foo.*,'sum')
			// target is like foo.bar or foo.*
			// def.Name is like foo.concretebar
			// so we want query to contain the concrete graphite name, potentially wrapped with consolidateBy().

			query := strings.Replace(queryForTarget[target], target, def.Name, -1)
			reqs = append(reqs, models.NewReq(def.Id, query, locdef.node, fromUnix, toUnix, maxDataPoints, uint32(def.Interval), consolidator))
		}
	}
	if len(reqs) == 0 {
		rbody.WriteResponse(ctx, rbody.NewJsonResponse(200, []string{}, ""))
		return
	}

	if (toUnix - fromUnix) >= logMinDur {
		log.Info("HTTP Render: INCOMING REQ %q from: %q, to: %q targets: %q, maxDataPoints: %q",
			ctx.Req.Method, from, to, request.Targets, request.MaxDataPoints)
	}

	reqs, err = alignRequests(reqs, s.MemoryStore.AggSettings())
	if err != nil {
		log.Error(3, "HTTP Render alignReq error: %s", err)
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
		return
	}

	if LogLevel < 2 {
		for _, req := range reqs {
			log.Debug("HTTP Render %s - arch:%d archI:%d outI:%d aggN: %d", req, req.Archive, req.ArchInterval, req.OutInterval, req.AggNum)
		}
	}

	out, err := s.getTargets(reqs)
	if err != nil {
		log.Error(3, "HTTP Render %s", err.Error())
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
		return
	}

	js := util.BufferPool.Get().([]byte)

	merged := mergeSeries(out)
	sort.Sort(models.SeriesByTarget(merged))
	js, err = models.SeriesByTarget(merged).GraphiteJSON(js)

	for _, serie := range out {
		pointSlicePool.Put(serie.Datapoints[:0])
	}
	if err != nil {
		util.BufferPool.Put(js[:0])
		log.Error(3, "HTTP Render %s", err.Error())
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
		return
	}

	reqHandleDuration.Value(time.Now().Sub(pre))
	rbody.WriteResponse(ctx, rbody.NewJsonResponse(200, json.RawMessage(js), ""))
	util.BufferPool.Put(js[:0])
}

func (s *Server) metricsFind(ctx *middleware.Context, request models.GraphiteFind) {
	if ctx.OrgId == 0 {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingOrgHeaderErr))
		return
	}

	if request.Query == "" {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingQueryErr))
		return
	}

	if request.Format != "" && request.Format != "treejson" && request.Format != "json" && request.Format != "completer" {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, InvalidFromatErr))
		return
	}
	// metricDefs only get updated periodically (when using CassandraIdx), so we add a 1day (86400seconds) buffer when
	// filtering by our From timestamp.  This should be moved to a configuration option,
	// but that will require significant refactoring to expose the updateInterval used
	// in the MetricIdx.  So this will have to do for now.
	if request.From != 0 {
		request.From -= 86400
	}
	nodes, err := s.MetricIndex.Find(ctx.OrgId, request.Query, request.From)
	if err != nil {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, err))
		return
	}
	var seenPaths map[string]struct{}
	peers := s.ClusterMgr.PeersForQuery()
	if len(peers) != 0 {
		seenPaths = make(map[string]struct{})
		for _, n := range nodes {
			seenPaths[n.Path] = struct{}{}
		}
	}

	for _, inst := range peers {
		if LogLevel < 2 {
			log.Debug("HTTP Find() querying %s/internal/index/find for %d:%s", inst.GetName(), ctx.OrgId, request.Query)
		}

		buf, err := inst.Post("/internal/index/find", models.IndexFind{Patterns: []string{request.Query}, OrgId: ctx.OrgId})
		if err != nil {
			log.Error(4, "HTTP Find() error querying %s/internal/index/find: %q", inst.GetName(), err)
			rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
			return
		}

		resp := models.NewIndexFindResp()
		for len(buf) != 0 {
			buf, err = resp.UnmarshalMsg(buf)
			if err != nil {
				log.Error(4, "HTTP Find() error unmarshaling body from %s/internal/index/find: %q", inst.GetName(), err)
				rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
				return
			}
			// different nodes may have overlapping data in their index.
			// maybe because they loaded the entire index from a persistent store,
			// or they used to receive a certain shard. or because they host metrics under branches
			// that other nodes also host metrics under
			// it may even happen that a node has a leaf that for another node is a branch, if the
			// org has been sending improper data.  in this case there's no elegant way to nicely handle this
			// so we'll just ignore one of them like we ignore other paths we've already seen.
			for _, n := range resp.Nodes[request.Query] {
				if _, ok := seenPaths[n.Path]; !ok {
					nodes = append(nodes, n)
					seenPaths[n.Path] = struct{}{}
				}
			}
		}
	}

	var b []byte
	switch request.Format {
	case "", "treejson", "json":
		b, err = findTreejson(request.Query, nodes)
	case "completer":
		b, err = findCompleter(nodes)
	}

	if err != nil {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
		return
	}
	rbody.WriteResponse(ctx, rbody.NewJsonResponse(200, json.RawMessage(b), request.Jsonp))
}

func (s *Server) metricsIndex(ctx *middleware.Context) {
	if ctx.OrgId == 0 {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingOrgHeaderErr))
		return
	}

	list := s.MetricIndex.List(ctx.OrgId)
	otherNodes := s.ClusterMgr.PeersForQuery()
	var seen map[string]struct{}
	if len(otherNodes) > 0 {
		seen = make(map[string]struct{})
		for _, def := range list {
			seen[def.Id] = struct{}{}
		}
	}
	for _, inst := range otherNodes {
		if LogLevel < 2 {
			log.Debug("HTTP IndexJson() querying %s/internal/index/list for %d", inst.RemoteAddr.String(), ctx.OrgId)
		}

		buf, err := inst.Post("/internal/index/list", models.IndexList{OrgId: ctx.OrgId})
		if err != nil {
			log.Error(4, "HTTP IndexJson() error querying %s/internal/index/list: %q", inst.GetName(), err)
			rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
			return
		}

		for len(buf) != 0 {
			var def schema.MetricDefinition
			buf, err = def.UnmarshalMsg(buf)
			if err != nil {
				log.Error(3, "HTTP IndexJson() error unmarshaling body from %s/internal/index/list: %q", inst.GetName(), err)
				rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
				return
			}
			// different nodes may have overlapping data in their index.
			// maybe because they loaded the entire index from a persistent store,
			// or they used to receive a certain shard.
			// so we need to filter out any duplicates
			_, ok := seen[def.Id]
			if !ok {
				list = append(list, def)
				seen[def.Id] = struct{}{}
			}
		}
	}
	var err error
	js := util.BufferPool.Get().([]byte)
	js, err = listJSON(js, list)
	if err != nil {
		log.Error(3, "HTTP IndexJson() %s", err.Error())
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
		util.BufferPool.Put(js[:0])
		return
	}
	rbody.WriteResponse(ctx, rbody.NewJsonResponse(200, json.RawMessage(js), ""))
	util.BufferPool.Put(js[:0])
}

func listJSON(b []byte, defs []schema.MetricDefinition) ([]byte, error) {
	seen := make(map[string]struct{})

	names := make([]string, 0, len(defs))

	for i := 0; i < len(defs); i++ {
		_, ok := seen[defs[i].Name]
		if !ok {
			names = append(names, defs[i].Name)
			seen[defs[i].Name] = struct{}{}
		}
	}
	sort.Strings(names)
	b = append(b, '[')
	for _, name := range names {
		b = append(b, '"')
		b = append(b, name...)
		b = append(b, `",`...)
	}
	if len(defs) != 0 {
		b = b[:len(b)-1] // cut last comma
	}
	b = append(b, ']')
	return b, nil
}

func parseTarget(target string) (string, string, error) {
	var consolidateBy string
	id := target
	// yes, i am aware of the arguably grossness of the below.
	// however, it is solid based on the documented allowed input format.
	// once we need to support several functions, we can implement
	// a proper expression parser
	if strings.HasPrefix(target, "consolidateBy(") {
		t := target
		if t[len(t)-2:] != "')" || (!strings.Contains(t, ",'") && !strings.Contains(t, ", '")) || strings.Count(t, "'") != 2 {
			return "", "", errors.New("target parse error")
		}
		consolidateBy = target[strings.Index(target, "'")+1 : strings.LastIndex(target, "'")]
		err := consolidation.Validate(consolidateBy)
		if err != nil {
			return "", "", err
		}

		id = target[strings.Index(target, "(")+1 : strings.LastIndex(target, ",")]
	}
	return id, consolidateBy, nil
}

type completer struct {
	Path   string `json:"path"`
	Name   string `json:"name"`
	IsLeaf string `json:"is_leaf"`
}

func findCompleter(nodes []idx.Node) ([]byte, error) {
	var b bytes.Buffer

	var complete = make([]completer, 0)

	for _, g := range nodes {
		c := completer{
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

		complete = append(complete, c)
	}

	err := json.NewEncoder(&b).Encode(struct {
		Metrics []completer `json:"metrics"`
	}{
		Metrics: complete},
	)
	return b.Bytes(), err
}

type treejson struct {
	AllowChildren int            `json:"allowChildren"`
	Expandable    int            `json:"expandable"`
	Leaf          int            `json:"leaf"`
	ID            string         `json:"id"`
	Text          string         `json:"text"`
	Context       map[string]int `json:"context"` // unused
}

var treejsonContext = make(map[string]int)

func findTreejson(query string, nodes []idx.Node) ([]byte, error) {
	var b bytes.Buffer

	tree := make([]treejson, 0)
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

		t := treejson{
			ID:      basepath + name,
			Context: treejsonContext,
			Text:    name,
		}

		if g.Leaf {
			t.Leaf = 1
		} else {
			t.AllowChildren = 1
			t.Expandable = 1
		}

		tree = append(tree, t)
	}

	err := json.NewEncoder(&b).Encode(tree)
	return b.Bytes(), err
}

func (s *Server) metricsDelete(ctx *middleware.Context, req models.MetricsDelete) {
	if ctx.OrgId == 0 {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingOrgHeaderErr))
		return
	}

	if req.Query == "" {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingQueryErr))
		return
	}

	defs, err := s.MetricIndex.Delete(ctx.OrgId, req.Query)
	if err != nil {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, err))
		return
	}

	resp := make(map[string]interface{})
	resp["success"] = true
	resp["deletedDefs"] = len(defs)
	rbody.WriteResponse(ctx, rbody.NewJsonResponse(200, resp, ""))
}
