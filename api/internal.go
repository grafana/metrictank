package api

import (
	"github.com/Unknwon/macaron"
)

func (s *Server) getData(ctx macaron.Context) {
	ctx.JSON(200, "ok")
}

func (s *Server) indexFind(ctx macaron.Context) {
	ctx.JSON(200, "ok")
}

func (s *Server) indexGet(ctx macaron.Context) {
	ctx.JSON(200, "ok")
}

func (s *Server) indexList(ctx macaron.Context) {
	ctx.JSON(200, "ok")
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

		orgs, ok := r.Form["org"]
		if !ok {
			http.Error(w, "missing org arg", http.StatusBadRequest)
			return
		}
		if len(orgs) != 1 {
			http.Error(w, "need exactly one org id", http.StatusBadRequest)
			return
		}
		org, err := strconv.Atoi(orgs[0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// metricDefs only get updated periodically (when using CassandraIdx), so we add a 1day (86400seconds) buffer when
		// filtering by our From timestamp.  This should be moved to a configuration option,
		// but that will require significant refactoring to expose the updateInterval used
		// in the MetricIdx.  So this will have to do for now.
		from, _ := strconv.ParseInt(r.FormValue("from"), 10, 64)
		if from != 0 {
			from -= 86400
		}

		var buf []byte
		nodes, err := metricIndex.Find(org, pattern, from)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, node := range nodes {
			buf, err = node.MarshalMsg(buf)
			if err != nil {
				log.Error(4, "HTTP IndexFind() marshal err: %q", err)
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
				log.Error(4, "HTTP IndexGet() marshal err: %q", err)
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

// IndexList returns msgp encoded schema.MetricDefinition's
func IndexList(metricIndex idx.MetricIndex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()

		orgs, ok := r.Form["org"]
		if !ok {
			http.Error(w, "missing org arg", http.StatusBadRequest)
			return
		}
		if len(orgs) != 1 {
			http.Error(w, "need exactly one org id", http.StatusBadRequest)
			return
		}
		org, err := strconv.Atoi(orgs[0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var buf []byte
		defs := metricIndex.List(org)
		for _, def := range defs {
			buf, err = def.MarshalMsg(buf)
			if err != nil {
				log.Error(4, "HTTP IndexList() marshal err: %q", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.Header().Set("Content-Type", "msgpack")
		w.Write(buf)
	}
}

func getData(store mdata.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pre := time.Now()
		var err error
		r.ParseForm()

		reqs, ok := r.Form["req"]
		if !ok {
			http.Error(w, "missing req arg", http.StatusBadRequest)
			return
		}
		if len(reqs) != 1 {
			http.Error(w, "need exactly one req", http.StatusBadRequest)
			return
		}
		var req Req
		err = json.Unmarshal([]byte(reqs[0]), &req)
		if !ok {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		req.loc = "local"
		points, interval, err := getTarget(store, req)
		if err != nil {
			log.Error(0, "HTTP getData() %s", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		series := Series{
			Target:     req.Target,
			Datapoints: points,
			Interval:   interval,
		}

		var buf []byte
		buf, err = series.MarshalMsg(buf)
		if err != nil {
			log.Error(0, "HTTP getData() %s", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqHandleDuration.Value(time.Now().Sub(pre))
		writeResponse(w, buf, httpTypeMsgp, "")
	}
}
