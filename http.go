package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/raintank/dur"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
	"math"
	"net/http"
	_ "net/http/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var errMetricNotFound = errors.New("metric not found")

var bufPool = sync.Pool{
	New: func() interface{} { return make([]byte, 0) },
}

type Series struct {
	Target     string // will be set to the target attribute of the given request
	Datapoints []schema.Point
	Interval   uint32
}

type SeriesByTarget []Series

func (g SeriesByTarget) Len() int           { return len(g) }
func (g SeriesByTarget) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g SeriesByTarget) Less(i, j int) bool { return g[i].Target < g[j].Target }

func corsHandler(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS")

		if r.Method == "OPTIONS" {
			// nothing to do, CORS headers already sent
			return
		}
		handler(w, r)
	}
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

// data output for graphite raintank target -> Target, datapoints -> Datapoints, and adds Interval field
func graphiteRaintankJSON(b []byte, series []Series) ([]byte, error) {
	b = append(b, '[')
	for _, s := range series {
		b = append(b, `{"Target":"`...)
		b = append(b, s.Target...)
		b = append(b, `","Datapoints":[`...)
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
		b = append(b, `],"Interval":`...)
		b = strconv.AppendInt(b, int64(s.Interval), 10)
		b = append(b, `},`...)
	}
	if len(series) != 0 {
		b = b[:len(b)-1] // cut last comma
	}
	b = append(b, ']')
	return b, nil
}

func getOrg(req *http.Request) (int, error) {
	orgStr := req.Header.Get("x-org-id")
	org, err := strconv.Atoi(orgStr)
	if err != nil {
		return 0, errors.New("bad org-id")
	}
	return org, nil
}

func IndexJson(metricIndex idx.MetricIndex) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		org, err := getOrg(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		list := metricIndex.List(org)
		js := bufPool.Get().([]byte)
		js, err = listJSON(js, list)
		if err != nil {
			log.Error(0, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			bufPool.Put(js[:0])
			return
		}
		writeResponse(w, js, httpTypeJSON, "")
		bufPool.Put(js[:0])
	}
}
func get(store mdata.Store, metricIndex idx.MetricIndex, aggSettings []mdata.AggSetting, logMinDur uint32) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		Get(w, req, store, metricIndex, aggSettings, logMinDur, false)
	}
}

func getLegacy(store mdata.Store, metricIndex idx.MetricIndex, aggSettings []mdata.AggSetting, logMinDur uint32) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		Get(w, req, store, metricIndex, aggSettings, logMinDur, true)
	}
}

func Get(w http.ResponseWriter, req *http.Request, store mdata.Store, metricIndex idx.MetricIndex, aggSettings []mdata.AggSetting, logMinDur uint32, legacy bool) {
	pre := time.Now()
	org := 0
	var err error
	if legacy {
		org, err = getOrg(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	req.ParseForm()

	maxDataPoints := uint32(800)
	maxDataPointsStr := req.Form.Get("maxDataPoints")
	if maxDataPointsStr != "" {
		tmp, err := strconv.Atoi(maxDataPointsStr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		maxDataPoints = uint32(tmp)
	}

	targets, ok := req.Form["target"]
	if !ok {
		http.Error(w, "missing target arg", http.StatusBadRequest)
		return
	}
	if *maxPointsPerReq != 0 && len(targets)*int(maxDataPoints) > *maxPointsPerReq {
		http.Error(w, "too many targets/maxDataPoints requested", http.StatusBadRequest)
		return
	}

	now := time.Now()

	from := req.Form.Get("from")
	to := req.Form.Get("to")
	if to == "" {
		to = req.Form.Get("until")
	}

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

	if legacy {
		// in MT, both the external and internal api, from is inclusive, to is exclusive
		// in graphite, from is exclusive and to inclusive
		// so in this case, adjust for internal api.
		fromUnix += 1
		toUnix += 1
	}
	if fromUnix >= toUnix {
		http.Error(w, "to must be higher than from", http.StatusBadRequest)
		return
	}
	if *maxDaysPerReq != 0 && len(targets)*int(toUnix-fromUnix) > *maxDaysPerReq*(3600*24) {
		http.Error(w, "too many targets/too large timeframe requested", http.StatusBadRequest)
		return
	}

	reqs := make([]Req, 0)
	for _, target := range targets {
		var consolidateBy string
		id := target
		// yes, i am aware of the arguably grossness of the below.
		// however, it is solid based on the documented allowed input format.
		// once we need to support several functions, we can implement
		// a proper expression parser
		if strings.HasPrefix(target, "consolidateBy(") {
			t := target
			if t[len(t)-2:] != "')" || (!strings.Contains(t, ",'") && !strings.Contains(t, ", '")) || strings.Count(t, "'") != 2 {
				http.Error(w, "target parse error", http.StatusBadRequest)
				return
			}
			consolidateBy = target[strings.Index(target, "'")+1 : strings.LastIndex(target, "'")]
			id = target[strings.Index(target, "(")+1 : strings.LastIndex(target, ",")]
		}

		if legacy {
			// metricDefs only get updated periodically, so we add a 1day (86400seconds) buffer when
			// filtering by our From timestamp.  This should be moved to a configuration option,
			// but that will require significant refactoring to expose the updateInterval used
			// in the MetricIdx.
			seenAfter := int64(fromUnix)
			if seenAfter != 0 {
				seenAfter -= 86400
			}
			nodes, err := metricIndex.Find(org, id, seenAfter)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if len(nodes) == 0 {
				http.Error(w, errMetricNotFound.Error(), http.StatusBadRequest)
				return
			}
			for _, node := range nodes {
				for _, def := range node.Defs {
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
					reqs = append(reqs, NewReq(def.Id, target, fromUnix, toUnix, maxDataPoints, uint32(def.Interval), consolidator))
				}
			}
		} else {
			// querying for a MT id
			def, err := metricIndex.Get(id)
			if err == idx.DefNotFound {
				e := fmt.Sprintf("metric %q not found", id)
				log.Error(0, e)
				http.Error(w, e, http.StatusBadRequest)
				return
			}
			consolidator, err := consolidation.GetConsolidator(&def, consolidateBy)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			reqs = append(reqs, NewReq(id, target, fromUnix, toUnix, maxDataPoints, uint32(def.Interval), consolidator))
		}
	}
	if (toUnix - fromUnix) >= logMinDur {
		log.Info("http.Get(): INCOMING REQ %q from: %q, to: %q targets: %q, maxDataPoints: %q",
			req.Method, req.Form.Get("from"), req.Form.Get("to"), req.Form["target"], req.Form.Get("maxDataPoints"))
	}

	if logLevel < 2 {
		for _, req := range reqs {
			log.Debug("HTTP Get() %s", req)
		}
	}

	reqs, err = alignRequests(reqs, aggSettings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	out, err := getTargets(store, reqs)
	if err != nil {
		log.Error(0, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	js := bufPool.Get().([]byte)
	if legacy {
		merged := mergeSeries(out)
		sort.Sort(SeriesByTarget(merged))
		js, err = graphiteJSON(js, merged)
	} else {
		// we dont merge here as graphite is expecting all metric.Ids it reqested.
		// graphite will then handle the merging itself.
		js, err = graphiteRaintankJSON(js, out)
	}
	for _, serie := range out {
		pointSlicePool.Put(serie.Datapoints[:0])
	}
	if err != nil {
		bufPool.Put(js[:0])
		log.Error(0, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqHandleDuration.Value(time.Now().Sub(pre))
	writeResponse(w, js, httpTypeJSON, "")
	bufPool.Put(js[:0])
}

// report ApplicationStatus for use by loadBalancer healthChecks.
// We only want requests to be sent to this node if it is the primary
// node or if it has been online for at *warmUpPeriod
func appStatus(w http.ResponseWriter, req *http.Request) {
	if mdata.CluStatus.IsPrimary() {
		w.Write([]byte("OK"))
		return
	}
	if time.Since(startupTime) < warmupPeriod {
		http.Error(w, "Service not ready", http.StatusServiceUnavailable)
		return
	}

	w.Write([]byte("OK"))
	return
}

func Find(metricIndex idx.MetricIndex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		writeResponse(w, b, httpTypeJSON, jsonp)
	}
}

func Delete(metricIndex idx.MetricIndex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.FormValue("query")
		org, err := getOrg(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if query == "" {
			http.Error(w, "missing parameter `query`", http.StatusBadRequest)
			return
		}

		defs, err := metricIndex.Delete(org, query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp := make(map[string]interface{})
		resp["success"] = true
		resp["deletedDefs"] = len(defs)
		b, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		writeResponse(w, b, httpTypeJSON, "")
	}
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
