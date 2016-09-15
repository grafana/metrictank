package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
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
func get(store mdata.Store, metricIndex idx.MetricIndex, aggSettings []mdata.AggSetting, logMinDur uint32, otherNodes []string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		Get(w, req, store, metricIndex, aggSettings, logMinDur, otherNodes, false)
	}
}

func getLegacy(store mdata.Store, metricIndex idx.MetricIndex, aggSettings []mdata.AggSetting, logMinDur uint32, otherNodes []string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		Get(w, req, store, metricIndex, aggSettings, logMinDur, otherNodes, true)
	}
}

func Get(w http.ResponseWriter, req *http.Request, store mdata.Store, metricIndex idx.MetricIndex, aggSettings []mdata.AggSetting, logMinDur uint32, otherNodes []string, legacy bool) {
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
		id, consolidateBy, err := parseTarget(target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		type locatedDef struct {
			def schema.MetricDefinition
			loc string
		}

		locatedDefs := make(map[string]locatedDef)

		if legacy {
			nodes, err := metricIndex.Find(org, id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			for _, node := range nodes {
				for _, def := range node.Defs {
					locatedDefs[def.Id] = locatedDef{def, "local"}
				}
			}

			for _, inst := range otherNodes {
				res, err := http.Get(inst)
				http.PostForm("http://%s/index/find", url.Values{"target": targets})
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				if res.StatusCode != 200 {
					// if the remote returned interval server error, or bad request, or whatever, we want to relay that as-is to the user.
					msg, _ := ioutil.ReadAll(res.Body)
					http.Error(w, string(msg), res.StatusCode)
				}
				defer res.Body.Close()
				buf, _ := ioutil.ReadAll(res.Body)
				var n idx.Node
				for len(buf) != 0 {
					buf, err = n.UnmarshalMsg(buf)
					// different nodes may have overlapping data in their index.
					// maybe because they loaded the entire index from a persistent store,
					// or they used to receive a certain shard.
					// so we need to select the node that has most recently seen each given metricDef.
					for _, def := range n.Defs {
						cur, ok := locatedDefs[def.Id]
						if ok && cur.def.LastUpdate > def.LastUpdate {
							continue
						}
						locatedDefs[def.Id] = locatedDef{def, inst}
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
		} else {
			// querying for a MT id
			def, err := metricIndex.Get(id)
			ok := err == nil
			loc := "local"
			for _, inst := range otherNodes {
				// if already have a very recent one, no need to look further
				if ok && def.LastUpdate > time.Now().Add(-20*time.Second).Unix() {
					break
				}
				res, err := http.Get(inst)
				http.PostForm("http://%s/index/get", url.Values{"id": []string{id}})
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				if res.StatusCode == 200 {
					defer res.Body.Close()
					buf, _ := ioutil.ReadAll(res.Body)
					var d schema.MetricDefinition
					buf, err := d.UnmarshalMsg(buf)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
					}
					// different nodes may have overlapping data in their index.
					// maybe because they loaded the entire index from a persistent store,
					// or they used to receive a certain shard.
					// so we need to select the node that has most recently seen each given metricDef.
					if ok && def.LastUpdate > d.LastUpdate {
						continue
					}
					def = d
					loc = inst
					ok = true

				} else if res.StatusCode == http.StatusNotFound {
					continue
				} else {
					// if the remote returned interval server error, or bad request, or whatever, we want to relay that as-is to the user.
					msg, _ := ioutil.ReadAll(res.Body)
					http.Error(w, string(msg), res.StatusCode)
				}
			}
			if !ok {
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
			reqs = append(reqs, NewReq(id, target, loc, fromUnix, toUnix, maxDataPoints, uint32(def.Interval), consolidator))
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

		nodes, err := metricIndex.Find(org, query)
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

// IndexFind returns a sequence of msgp encoded idx.Node's
func IndexFind(metricIndex idx.MetricIndex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		patterns, ok := r.Form["pattern"]
		if !ok {
			http.Error(w, "missing pattern arg", http.StatusBadRequest)
			return
		}
		if len(patterns) != 1 {
			http.Error(w, "need exactly one pattern", http.StatusBadRequest)
			return
		}
		pattern := patterns[0]
		org, err := getOrg(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var buf []byte
		nodes, err := metricIndex.Find(org, pattern)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, node := range nodes {
			buf, err = node.MarshalMsg(buf)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", "msgpack")
		w.Write(buf)
	}
}

// IndexGet returns a msgp encoded schema.MetricDefinition
func IndexGet(metricIndex idx.MetricIndex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		ids, ok := r.Form["id"]
		if !ok {
			http.Error(w, "missing id arg", http.StatusBadRequest)
			return
		}
		if len(ids) != 1 {
			http.Error(w, "need exactly one id", http.StatusBadRequest)
			return
		}
		id := ids[0]

		var buf []byte
		def, err := metricIndex.Get(id)
		if err == nil {
			buf, err = def.MarshalMsg(buf)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "msgpack")
			w.Write(buf)
		} else { // currently this can only be notFound
			http.Error(w, "not found", http.StatusNotFound)
		}

	}
}
