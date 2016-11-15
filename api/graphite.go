package api

import (
	"errors"
	"net/http"
	"sort"
	"strings"
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
			return "", "", errors.New("target parse error")
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

func (s *Server) renderMetrics(ctx *middleware.Context, request models.GraphiteRender) {
	pre := time.Now()

	targets := request.Targets
	if maxPointsPerReq != 0 && len(targets)*int(request.MaxDataPoints) > maxPointsPerReq {
		response.Write(ctx, response.NewError(http.StatusForbidden, MaxPointsPerReqErr))
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
		response.Write(ctx, response.NewError(http.StatusBadRequest, err))
		return
	}

	toUnix, err := dur.ParseTSpec(to, now, defaultTo)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err))
		return
	}

	// in MT, both the external and internal api, from is inclusive, to is exclusive
	// in graphite, from is exclusive and to inclusive
	// so in this case, adjust for internal api.
	fromUnix += 1
	toUnix += 1

	if fromUnix >= toUnix {
		response.Write(ctx, response.NewError(http.StatusBadRequest, InvalidTimeRangeErr))
		return
	}
	if maxDaysPerReq != 0 && len(targets)*int(toUnix-fromUnix) > maxDaysPerReq*(3600*24) {
		response.Write(ctx, response.NewError(http.StatusForbidden, MaxDaysPerReqErr))
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
			response.Write(ctx, response.NewError(http.StatusBadRequest, err))
			return
		}
		for _, node := range nodes {
			for _, def := range node.Defs {
				locatedDefs[target][def.Id] = locatedDef{def, cluster.ThisNode}
			}
		}
	}

	for target, ldefs := range locatedDefs {
		for _, locdef := range ldefs {
			def := locdef.def
			consolidator, err := consolidation.GetConsolidator(&def, parsedTargets[target])
			if err != nil {
				response.Write(ctx, response.NewError(http.StatusBadRequest, err))
				return
			}
			// query is like foo.bar or foo.* or consolidateBy(foo.*,'sum')
			// target is like foo.bar or foo.*
			// def.Name is like foo.concretebar
			// so we want query to contain the concrete graphite name, potentially wrapped with consolidateBy().

			query := strings.Replace(queryForTarget[target], target, def.Name, -1)
			reqs = append(reqs, models.NewReq(def.Id, query, fromUnix, toUnix, request.MaxDataPoints, uint32(def.Interval), consolidator))
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
		response.Write(ctx, response.NewError(http.StatusInternalServerError, err))
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
		response.Write(ctx, response.NewError(http.StatusInternalServerError, err))
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
	nodes, err := s.MetricIndex.Find(ctx.OrgId, request.Query, request.From)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err))
		return
	}

	var b interface{}
	switch request.Format {
	case "", "treejson", "json":
		b, err = findTreejson(request.Query, nodes)
	case "completer":
		b, err = findCompleter(nodes)
	}

	if err != nil {
		response.Write(ctx, response.NewError(http.StatusInternalServerError, err))
		return
	}
	response.Write(ctx, response.NewJson(200, b, request.Jsonp))
}

func (s *Server) metricsIndex(ctx *middleware.Context) {
	list := s.MetricIndex.List(ctx.OrgId)
	response.Write(ctx, response.NewFastJson(200, models.MetricNames(list)))
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

		t := models.SeriesTreeItem{
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

		tree.Add(&t)
	}
	return *tree, nil
}

func (s *Server) metricsDelete(ctx *middleware.Context, req models.MetricsDelete) {
	if ctx.OrgId == 0 {
		response.Write(ctx, response.NewError(http.StatusBadRequest, MissingOrgHeaderErr))
		return
	}

	if req.Query == "" {
		response.Write(ctx, response.NewError(http.StatusBadRequest, MissingQueryErr))
		return
	}

	defs, err := s.MetricIndex.Delete(ctx.OrgId, req.Query)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err))
		return
	}

	resp := make(map[string]interface{})
	resp["success"] = true
	resp["deletedDefs"] = len(defs)
	response.Write(ctx, response.NewJson(200, resp, ""))
}
