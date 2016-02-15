package main

import (
	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"math"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
)

var bufPool = sync.Pool{
	New: func() interface{} { return make([]byte, 0) },
}

type Point struct {
	Val float64
	Ts  uint32
}

type Series struct {
	Target     string
	Datapoints []Point
	Interval   uint32
}

func graphiteJSON(b []byte, series []Series) ([]byte, error) {
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
		b = b[:len(b)-1] // cut last comma
		b = append(b, `],"Interval":`...)
		b = strconv.AppendInt(b, int64(s.Interval), 10)
		b = append(b, `},`...)
	}
	b = b[:len(b)-1] // cut last comma
	b = append(b, ']')
	return b, nil
}

func get(store Store, defCache *DefCache, aggSettings []aggSetting) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		Get(w, req, store, defCache, aggSettings)
	}
}

// note: we don't normalize/quantize/fill-unknowns
// we just serve what we know
func Get(w http.ResponseWriter, req *http.Request, store Store, defCache *DefCache, aggSettings []aggSetting) {
	pre := time.Now()
	req.ParseForm()
	log.Debug(fmt.Sprintf("http.Get(): INCOMING REQ. targets: %q, maxDataPoints: %q", req.Form.Get("target"), req.Form.Get("maxDataPoints")))

	maxDataPoints := uint32(800)
	maxDataPointsStr := req.Form.Get("maxDataPoints")
	var err error
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
	now := time.Now()
	fromUnix := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	toUnix := uint32(now.Add(time.Duration(1) * time.Second).Unix())
	from := req.Form.Get("from")
	if from != "" {
		fromUnixInt, err := strconv.Atoi(from)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fromUnix = uint32(fromUnixInt)
	}
	to := req.Form.Get("to")
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

	reqs := make([]Req, len(targets))
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
			def, ok := defCache.Get(id)
			consolidateBy = "avg"
			if ok && def.TargetType == "counter" {
				consolidateBy = "last"
			}
		}
		var consolidator consolidation.Consolidator
		switch consolidateBy {
		case "avg", "average":
			consolidator = consolidation.Avg
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
		req := NewReq(id, fromUnix, toUnix, maxDataPoints, consolidator)
		reqs[i] = req
	}
	err = findMetricsForRequests(reqs, defCache)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reqs, err = alignRequests(reqs, aggSettings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	out := make([]Series, len(reqs))
	for i, req := range reqs {
		log.Debug("===================================")
		log.Debug("HTTP Get()          %s", req)
		points, interval, err := getTarget(store, req)
		if err != nil {
			log.Error(0, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		out[i] = Series{
			Target:     targets[i],
			Datapoints: points,
			Interval:   interval,
		}
	}
	js := bufPool.Get().([]byte)
	js, err = graphiteJSON(js, out)
	if err != nil {
		log.Error(0, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bufPool.Put(js[:0])

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
