package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sort"
	"sync"

	"github.com/Unknwon/macaron"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/worldping-api/pkg/log"
)

var bufPool = sync.Pool{
	New: func() interface{} { return make([]byte, 0) },
}

func (s *Server) renderMetrics(ctx macaron.Context, request models.GraphiteRender) {
	maxDataPoints := uint32(800)
	maxDataPointsStr := req.Form.Get("maxDataPoints")
	if request.MaxDataPoints != 0 {
		maxDataPoints = request.MaxDataPoints
	}

	targets := request.Targets
	if *maxPointsPerReq != 0 && len(targets)*int(maxDataPoints) > *maxPointsPerReq {
		ctx.Error(http.StatusBadRequest, "too many targets/maxDataPoints requested")
		return
	}

	now := time.Now()

	from := request.From
	to := request.Until

	defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	defaultTo := uint32(now.Add(time.Duration(1) * time.Second).Unix())

	fromUnix, err := dur.ParseTSpec(from, now, defaultFrom)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	toUnix, err := dur.ParseTSpec(to, now, defaultTo)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// in MT, both the external and internal api, from is inclusive, to is exclusive
	// in graphite, from is exclusive and to inclusive
	// so in this case, adjust for internal api.
	fromUnix += 1
	toUnix += 1

	if fromUnix >= toUnix {
		ctx.Error(http.StatusBadRequest, "to must be higher than from")
		return
	}
	if *maxDaysPerReq != 0 && len(targets)*int(toUnix-fromUnix) > *maxDaysPerReq*(3600*24) {
		ctx.Error(http.StatusBadRequest, "too many targets/too large timeframe requested")
		return
	}

	reqs := make([]Req, 0)
	for _, target := range targets {
		id, consolidateBy, err := parseTarget(target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		type locatedDef struct {
			def schema.MetricDefinition
			loc string
		}

		locatedDefs := make(map[string]locatedDef)

		// metricDefs only get updated periodically, so we add a 1day (86400seconds) buffer when
		// filtering by our From timestamp.  This should be moved to a configuration option,
		// but that will require significant refactoring to expose the updateInterval used
		// in the MetricIdx.
		seenAfter := int64(fromUnix)
		if seenAfter != 0 {
			seenAfter -= 86400
		}
		nodes, err := s.MetricIndex.Find(org, id, seenAfter)
		if err != nil {
			ctx.Error(http.StatusBadRequest, err.Error())
			return
		}
		for _, node := range nodes {
			for _, def := range node.Defs {
				locatedDefs[def.Id] = locatedDef{def, "local"}
			}
		}

		for _, inst := range s.ClusterMgr.PeersForQuery() {
			if log.LogLevel < 2 {
				log.Debug("HTTP Get() querying %s/internal/index/find for %d:%s", inst.RemoteAddr.String(), ctx.OrgId, target)
			}

			res, err := http.PostForm(fmt.Sprintf("%s/internal/index/find", inst.RemoteAddr.String()), url.Values{"pattern": []string{target}, "org": []string{fmt.Sprintf("%d", ctx.OrgId)}})
			if err != nil {
				log.Error(4, "HTTP Get() error querying %s/internal/index/find: %q", inst.RemoteAddr.String(), err)
				ctx.Error(http.StatusInternalServerError, err.Error())
				return
			}
			defer res.Body.Close()
			buf, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Error(4, "HTTP Get() error reading body from %s/internal/index/find: %q", inst.RemoteAddr.String(), err)
				ctx.Error(http.StatusInternalServerError, err.Error())
				return
			}
			if res.StatusCode != 200 {
				// if the remote returned interval server error, or bad request, or whatever, we want to relay that as-is to the user.
				log.Error(4, "HTTP Get() %s/internal/index/find returned http %d: %v", inst.RemoteAddr.String(), res.StatusCode, string(buf))
				http.Error(w, string(buf), res.StatusCode)
				return
			}
			var n idx.Node
			for len(buf) != 0 {
				buf, err = n.UnmarshalMsg(buf)
				if err != nil {
					log.Error(4, "HTTP Get() error unmarshaling body from %s/internal/index/find: %q", inst.RemoteAddr.String(), err)
					ctx.Error(http.StatusInternalServerError, err.Error())
					return
				}
				// different nodes may have overlapping data in their index.
				// maybe because they loaded the entire index from a persistent store,
				// or they used to receive a certain shard.
				// so we need to select the node that has most recently seen each given metricDef.
				for _, def := range n.Defs {
					cur, ok := locatedDefs[def.Id]
					if ok && cur.def.LastUpdate >= def.LastUpdate {
						continue
					}
					locatedDefs[def.Id] = locatedDef{def, inst.RemoteAddr.String()}
				}
			}
		}
		if len(locatedDefs) == 0 {
			http.Error(w, errMetricNotFound.Error(), http.StatusBadRequest)
			return
		}
		for _, locdef := range locatedDefs {
			def := locdef.def
			consolidator, err := consolidation.GetConsolidator(&def, consolidateBy)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			// target is like foo.bar or foo.* or consolidateBy(foo.*,'sum')
			// id is like foo.bar or foo.*
			// def.Name is like foo.concretebar
			// so we want target to contain the concrete graphite name, potentially wrapped with consolidateBy().
			target := strings.Replace(target, id, def.Name, -1)
			reqs = append(reqs, NewReq(def.Id, target, locdef.loc, fromUnix, toUnix, maxDataPoints, uint32(def.Interval), consolidator))
		}

	}
	if (toUnix - fromUnix) >= logMinDur {
		log.Info("HTTP Get(): INCOMING REQ %q from: %q, to: %q targets: %q, maxDataPoints: %q",
			req.Method, req.Form.Get("from"), req.Form.Get("to"), req.Form["target"], req.Form.Get("maxDataPoints"))
	}

	reqs, err = alignRequests(reqs, aggSettings)
	if err != nil {
		log.Error(4, "HTTP Get() alignReq error: %s", err)
		ctx.Error(http.StatusInternalServerError, err.Error())
		return
	}

	if log.LogLevel < 2 {
		for _, req := range reqs {
			log.Debug("HTTP Get() %s - arch:%d archI:%d outI:%d aggN: %d", req, req.Archive, req.ArchInterval, req.OutInterval, req.AggNum)
		}
	}

	out, err := getTargets(store, reqs)
	if err != nil {
		log.Error(0, "HTTP Get() %s", err.Error())
		ctx.Error(http.StatusInternalServerError, err.Error())
		return
	}

	js := bufPool.Get().([]byte)

	merged := mergeSeries(out)
	sort.Sort(SeriesByTarget(merged))
	js, err = graphiteJSON(js, merged)

	for _, serie := range out {
		pointSlicePool.Put(serie.Datapoints[:0])
	}
	if err != nil {
		bufPool.Put(js[:0])
		log.Error(0, "HTTP Get() %s", err.Error())
		ctx.Error(http.StatusInternalServerError, err.Error())
		return
	}

	reqHandleDuration.Value(time.Now().Sub(pre))
	rbody.writeResponse(ctx, js, httpTypeJSON, "")
	bufPool.Put(js[:0])
}

func (s *Server) metricsFind(ctx macaron.Context, request models.GraphiteFind) {
	format := r.FormValue("format")
	jsonp := r.FormValue("jsonp")
	query := r.FormValue("query")
	from, _ := strconv.ParseInt(r.FormValue("from"), 10, 64)

	org, err := getOrg(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if query == "" {
		http.Error(w, "missing parameter `query`", http.StatusBadRequest)
		return
	}

	if format != "" && format != "treejson" && format != "json" && format != "completer" {
		http.Error(w, "invalid format", http.StatusBadRequest)
		return
	}
	// metricDefs only get updated periodically (when using CassandraIdx), so we add a 1day (86400seconds) buffer when
	// filtering by our From timestamp.  This should be moved to a configuration option,
	// but that will require significant refactoring to expose the updateInterval used
	// in the MetricIdx.  So this will have to do for now.
	if from != 0 {
		from -= 86400
	}
	nodes, err := metricIndex.Find(org, query, from)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var seenPaths map[string]struct{}
	if len(otherNodes) != 0 {
		seenPaths = make(map[string]struct{})
		for _, n := range nodes {
			seenPaths[n.Path] = struct{}{}
		}
	}

	for _, inst := range otherNodes {
		if log.LogLevel < 2 {
			log.Debug("HTTP Find() querying %s/internal/index/find for %d:%s", inst, org, query)
		}

		res, err := http.PostForm(fmt.Sprintf("http://%s/internal/index/find", inst), url.Values{"pattern": []string{query}, "org": []string{fmt.Sprintf("%d", org)}})
		if err != nil {
			log.Error(4, "HTTP Find() error querying %s/internal/index/find: %q", inst, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer res.Body.Close()
		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Error(4, "HTTP Find() error reading body from %s/internal/index/find: %q", inst, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if res.StatusCode != 200 {
			// if the remote returned interval server error, or bad request, or whatever, we want to relay that as-is to the user.
			log.Error(4, "HTTP Find() %s/internal/index/find returned http %d: %v", inst, res.StatusCode, string(buf))
			http.Error(w, string(buf), res.StatusCode)
			return
		}
		var n idx.Node
		for len(buf) != 0 {
			buf, err = n.UnmarshalMsg(buf)
			if err != nil {
				log.Error(4, "HTTP Find() error unmarshaling body from %s/internal/index/find: %q", inst, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// different nodes may have overlapping data in their index.
			// maybe because they loaded the entire index from a persistent store,
			// or they used to receive a certain shard. or because they host metrics under branches
			// that other nodes also host metrics under
			// it may even happen that a node has a leaf that for another node is a branch, if the
			// org has been sending improper data.  in this case there's no elegant way to nicely handle this
			// so we'll just ignore one of them like we ignore other paths we've already seen.
			_, ok := seenPaths[n.Path]
			if !ok {
				nodes = append(nodes, n)
				seenPaths[n.Path] = struct{}{}
			}
		}
	}

	var b []byte
	switch format {
	case "", "treejson", "json":
		b, err = findTreejson(query, nodes)
	case "completer":
		b, err = findCompleter(nodes)
	}

	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	rbody.writeResponse(ctx, b, httpTypeJSON, jsonp)
}

func (s *Server) metricsIndex(ctx macaron.Context) {
	list := s.MetricIndex.List(org)
	otherNodes := s.ClusterMgr.PeersForQuery()
	var seen map[string]struct{}
	if len(otherNodes) > 0 {
		seen = make(map[string]struct{})
		for _, def := range list {
			seen[def.Id] = struct{}{}
		}
	}
	for _, inst := range otherNodes {
		if log.LogLevel < 2 {
			log.Debug("HTTP IndexJson() querying %s/internal/index/list for %d", inst.RemoteAddr.String(), ctx.OrgId)
		}

		res, err := http.PostForm(fmt.Sprintf("%s/internal/index/list", inst.RemoteAddr.String()), url.Values{"org": []string{fmt.Sprintf("%d", ctx.OrgId)}})
		if err != nil {
			log.Error(4, "HTTP IndexJson() error querying %s/internal/index/list: %q", inst, err)
			ctx.Error(http.StatusInternalServerError, err.Error())
			return
		}
		defer res.Body.Close()
		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Error(4, "HTTP IndexJson() error reading body from %s/internal/index/list: %q", inst.RemoteAddr.String(), err)
			ctx.Error(http.StatusInternalServerError, err.Error())
			return
		}
		if res.StatusCode != 200 {
			// if the remote returned interval server error, or bad request, or whatever, we want to relay that as-is to the user.
			log.Error(4, "HTTP IndexJson() %s/internal/index/list returned http %d: %v", inst.RemoteAddr.String(), res.StatusCode, string(buf))
			ctx.Error(res.StatusCode, string(buf))
			return
		}
		for len(buf) != 0 {
			var def schema.MetricDefinition
			buf, err = def.UnmarshalMsg(buf)
			if err != nil {
				log.Error(4, "HTTP IndexJson() error unmarshaling body from %s/internal/index/list: %q", inst.RemoteAddr.String(), err)
				ctx.Error(http.StatusInternalServerError, err.Error())
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
	js := bufPool.Get().([]byte)
	js, err = listJSON(js, list)
	if err != nil {
		log.Error(0, "HTTP IndexJson() %s", err.Error())
		ctx.Error(http.StatusInternalServerError, err.Error())
		bufPool.Put(js[:0])
		return
	}
	rbody.writeResponse(ctx, js, httpTypeJSON, "")
	bufPool.Put(js[:0])
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

// regular graphite output
func graphiteJSON(b []byte, series []Series) ([]byte, error) {
	b = append(b, '[')
	for _, s := range series {
		b = append(b, `{"target":"`...)
		b = append(b, s.Target...)
		b = append(b, `","datapoints":[`...)
		for _, p := range s.Datapoints {
			b = append(b, '[')
			if math.IsNaN(p.Val) {
				b = append(b, `null,`...)
			} else {
				b = strconv.AppendFloat(b, p.Val, 'f', 3, 64)
				b = append(b, ',')
			}
			b = strconv.AppendUint(b, uint64(p.Ts), 10)
			b = append(b, `],`...)
		}
		if len(s.Datapoints) != 0 {
			b = b[:len(b)-1] // cut last comma
		}
		b = append(b, `]},`...)
	}
	if len(series) != 0 {
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
