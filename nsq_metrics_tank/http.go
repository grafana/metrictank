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
	fromUnix := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	toUnix := uint32(now.Add(time.Duration(1) * time.Second).Unix())
	from := values.Get("from")
	if from != "" {
		fromUnixInt, err := strconv.Atoi(from)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fromUnix = uint32(fromUnixInt)
	}
	to := values.Get("to")
	if to != "" {
		toUnixInt, err := strconv.Atoi(to)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		toUnix = uint32(toUnixInt)
	}
	if fromUnix >= toUnix {
		http.Error(w, "to must be higher than from", http.StatusBadRequest)
		return
	}

	out := make([]Series, len(keys))
	for i, key := range keys {
		iters := make([]*tsz.Iter, 0)
		var memIters []*tsz.Iter
		oldest := toUnix
		if metric, ok := metrics.Get(key); ok {
			oldest, memIters = metric.Get(fromUnix, toUnix)
		} else {
			memIters = make([]*tsz.Iter, 0)
		}
		if oldest > fromUnix {
			log.Println("data load from cassandra:", TS(fromUnix), "-", TS(oldest), " from mem:", TS(oldest), "- ", TS(toUnix))
			storeIters, err := searchCassandra(key, fromUnix, oldest)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			for _, i := range storeIters {
				fmt.Println("c>", TS(i.T0()))
			}
			iters = append(iters, storeIters...)
		} else {
			log.Println("data load from mem:", TS(fromUnix), "-", TS(toUnix), "oldest (", oldest, ")")
		}
		iters = append(iters, memIters...)
		for _, i := range memIters {
			fmt.Println("m>", TS(i.T0()))
		}
		points := make([]Point, 0)
		for _, iter := range iters {
			for iter.Next() {
				ts, val := iter.Values()
				if ts >= fromUnix && ts < toUnix {
					points = append(points, Point{val, ts})
				}
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

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}
