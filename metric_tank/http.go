package main

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"math"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"time"
)

type Point struct {
	Val float64
	Ts  uint32
}

func (p *Point) MarshalJSON() ([]byte, error) {
	if math.IsNaN(p.Val) {
		return []byte(fmt.Sprintf("[null, %d]", p.Ts)), nil
	}
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
	log.Debug(fmt.Sprintf("http.Get(): INCOMING REQ. targets: %q, maxDataPoints: %q", values.Get("target"), values.Get("maxDataPoints")))

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

	targets, ok := values["target"]
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
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fromUnix = uint32(fromUnixInt)
	}
	to := values.Get("to")
	if to != "" {
		toUnixInt, err := strconv.Atoi(to)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		toUnix = uint32(toUnixInt)
	}
	if fromUnix >= toUnix {
		http.Error(w, "to must be higher than from", http.StatusBadRequest)
		return
	}

	out := make([]Series, len(targets))
	for i, target := range targets {
		var consolidateBy string
		id := target
		// yes, i am aware of the arguably grossness of the below.
		// however, it is solid based on the documented allowed input format.
		// once we need to support several functions, we can implement
		// a proper expression parser
		if strings.HasPrefix(target, "consolidateBy(") {
			if target[len(target)-2:] != "')" || !strings.Contains(target, ",'") || strings.Count(target, "'") != 2 || strings.Count(target, ",") != 1 {
				http.Error(w, "target parse error", http.StatusBadRequest)
				return
			}
			consolidateBy = target[strings.Index(target, "'")+1 : strings.LastIndex(target, "'")]
			id = target[strings.Index(target, "(")+1 : strings.Index(target, ",")]
		}

		if consolidateBy == "" {
			meta := metaCache.Get(id)
			consolidateBy = "avg"
			if meta.targetType == "counter" {
				consolidateBy = "last"
			}
		}
		var consolidator consolidation.Consolidator
		switch consolidateBy {
		case "avg", "average":
			consolidator = consolidation.Avg
		case "last":
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
		log.Debug("===================================")
		req := NewReq(id, fromUnix, toUnix, minDataPoints, maxDataPoints, consolidator)
		log.Debug("HTTP Get()          %s", req)
		points, err := getTarget(req, aggSettings, metaCache)
		if err != nil {
			log.Error(0, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		out[i] = Series{
			Target:     target,
			Datapoints: points,
		}
	}
	js, err := json.Marshal(out)
	if err != nil {
		log.Error(0, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	reqHandleDuration.Value(time.Now().Sub(pre))
	w.Write(js)
}

// report ApplicationStatus for use by loadBalancer healthChecks.
// We only want requests to be sent to this node if it is the primary
// node or if it has been online for at *warmUpPeriod
func appStatus(w http.ResponseWriter, req *http.Request) {
	if clusterStatus.IsPrimary() {
		w.Write([]byte("OK"))
		return
	}
	if time.Since(startupTime) < (time.Duration(*warmUpPeriod) * time.Second) {
		http.Error(w, "Service not ready", http.StatusServiceUnavailable)
		return
	}

	w.Write([]byte("OK"))
	return
}
