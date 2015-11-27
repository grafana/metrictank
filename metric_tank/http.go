package main

import (
	"encoding/json"
	"fmt"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
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

func get(metaCache *MetaCache, aggSettings []aggSetting) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		Get(w, req, metaCache, aggSettings)
	}
}

// note: we don't normalize/quantize/fill-unknowns
// we just serve what we know
func Get(w http.ResponseWriter, req *http.Request, metaCache *MetaCache, aggSettings []aggSetting) {
	pre := time.Now()
	values := req.URL.Query()

	consolidateBy := values.Get("consolidateBy")
	var consolidator consolidation.Consolidator
	switch consolidateBy {
	case "avg", "average":
		consolidator = consolidation.Avg
	case "", "last":
		consolidator = consolidation.Last
	case "min":
		consolidator = consolidation.Min
	case "max":
		consolidator = consolidation.Max
	case "sum":
		consolidator = consolidation.Sum
	default:
		http.Error(w, "unrecognized consolidation function", http.StatusBadRequest)
		return
	}

	maxDataPoints := uint32(800)
	maxDataPointsStr := values.Get("maxDataPoints")
	var err error
	if maxDataPointsStr != "" {
		tmp, err := strconv.Atoi(maxDataPointsStr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		maxDataPoints = uint32(tmp)
	}
	minDataPoints := maxDataPoints / 10

	keys, ok := values["target"]
	if !ok {
		http.Error(w, "missing target arg", http.StatusBadRequest)
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
		points, err := getTarget(key, fromUnix, toUnix, minDataPoints, maxDataPoints, consolidator, aggSettings)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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
	reqHandleDuration.Value(time.Now().Sub(pre))
	w.Write(js)
}
