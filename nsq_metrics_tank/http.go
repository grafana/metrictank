package main

import (
	"encoding/json"
	"fmt"
	"github.com/dgryski/go-tsz"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"time"
)

type Point struct {
	Val float64
	Ts  uint32
}

func (p *Point) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("[%d, %f]", p.Val, p.Ts)), nil
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
		iters := make([]*tsz.Iter, 0)
		metric := metrics.Get(key)
		oldest, memIters, err := metric.GetUnsafe(uint32(fromUnix), uint32(time.Now().Unix()))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if oldest > uint32(fromUnix) {
			log.Println(key, "mem:", oldest, "- now.  cassandra:", fromUnix, "-", oldest)
			storeIters, err := searchCassandra(key, uint32(fromUnix), oldest)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			iters = append(iters, storeIters...)
		} else {
			log.Println(key, "mem:", oldest, "- now")
		}
		iters = append(iters, memIters...)
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
