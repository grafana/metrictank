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
	return []byte(fmt.Sprintf("[%f, %d]", p.Val, p.Ts)), nil
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
	now := time.Now()
	fromUnix := now.Add(-time.Duration(24) * time.Hour).Unix()
	toUnix := now.Add(time.Duration(1) * time.Second).Unix()
	from := values.Get("from")
	if from != "" {
		fromUnixInt, err := strconv.Atoi(from)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fromUnix = int64(fromUnixInt)
	}
	to := values.Get("to")
	if to != "" {
		toUnixInt, err := strconv.Atoi(to)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		toUnix = int64(toUnixInt)
	}
	out := make([]Series, len(keys))
	for i, key := range keys {
		iters := make([]*tsz.Iter, 0)
		metric := metrics.Get(key)
		oldest, memIters, err := metric.GetUnsafe(uint32(fromUnix), uint32(toUnix))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if oldest > uint32(fromUnix) {
			log.Println("data load from cassandra:", TS(fromUnix), "-", TS(oldest), " from mem:", TS(oldest), "- ", TS(toUnix))
			storeIters, err := searchCassandra(key, uint32(fromUnix), oldest)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			iters = append(iters, storeIters...)
		} else {
			log.Println("data load from mem:", TS(fromUnix), "-", TS(toUnix))
		}
		// TODO filter out points we didn't ask for
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
