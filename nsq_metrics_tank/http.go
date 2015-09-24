package main

import (
	"encoding/json"
	"strconv"
	"time"

	"net/http"
	_ "net/http/pprof"
)

type Point struct {
	Val float64
	Ts  uint32
}

type Series struct {
	Target     string
	Datapoints []Point
}

// note: we don't normalize/quantize/fill-unknowns
// we just serve what we know
func Get(w http.ResponseWriter, req *http.Request) {
	values := req.URL.Query()
	keys, ok := values["render"]
	if !ok {
		http.Error(w, "missing render arg", http.StatusBadRequest)
		return
	}
	fromUnix := time.Now().Add(-time.Duration(24) * time.Hour).Unix()
	from := values.Get("from")
	if from != "" {
		fromUnixInt, err := strconv.Atoi(from)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fromUnix = int64(fromUnixInt)
	}
	out := make([]Series, len(keys))
	for i, key := range keys {
		metric := metrics.Get(key)
		iters, err := metric.GetUnsafe(uint32(fromUnix), uint32(time.Now().Unix()))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		points := make([]Point, 0)
		for _, iter := range iters {
			for iter.Next() {
				ts, val := iter.Values()
				points = append(points, Point{val, ts})
			}
		}
		out[i] = Series{
			Target:     key,
			Datapoints: points,
		}
	}
	js, err := json.Marshal(out)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// doesn't exactly match standard graphite output, not sure how to do it like arrays
	// [{"target": "error", "datapoints": [[null, 1443070801]], ...}]

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}
