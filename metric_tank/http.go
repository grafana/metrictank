package main

import (
	"errors"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/dur"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"github.com/raintank/raintank-metric/schema"
	"math"
	"net/http"
	_ "net/http/pprof"
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
	Target     string
	Datapoints []schema.Point
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

func get(store Store, defCache *DefCache, aggSettings []aggSetting, logMinDur uint32) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		Get(w, req, store, defCache, aggSettings, logMinDur, false)
	}
}

func getLegacy(store Store, defCache *DefCache, aggSettings []aggSetting, logMinDur uint32) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		Get(w, req, store, defCache, aggSettings, logMinDur, true)
	}
}

// note: we don't normalize/quantize/fill-unknowns
// we just serve what we know
func Get(w http.ResponseWriter, req *http.Request, store Store, defCache *DefCache, aggSettings []aggSetting, logMinDur uint32, legacy bool) {
	pre := time.Now()
	req.ParseForm()

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
	if len(targets)*int(maxDataPoints) > 500*2000 {
		http.Error(w, "too many targets/maxDataPoints requested", http.StatusBadRequest)
		return
	}

	now := time.Now()
	fromUnix := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	toUnix := uint32(now.Add(time.Duration(1) * time.Second).Unix())
	from := req.Form.Get("from")
	if from != "" {
		fromUnixInt, err := strconv.Atoi(from)
		if err == nil {
			fromUnix = uint32(fromUnixInt)
		} else {
			if len(from) == 1 || from[0] != '-' {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			dur, err := dur.ParseUNsec(from[1:])
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			fromUnix = uint32(now.Add(-time.Duration(dur) * time.Second).Unix())
		}
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
	if len(targets)*int(toUnix-fromUnix) > 500*2*365*24*3600 {
		http.Error(w, "too many targets/too large timeframe requested", http.StatusBadRequest)
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

		// if we're serving the legacy graphite api, set the id field by looking up the graphite target
		if legacy {
			def, ok := defCache.GetByKey(id)
			if !ok {
				http.Error(w, errMetricNotFound.Error(), http.StatusInternalServerError)
				return
			}
			id = def.Id
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
		req := NewReq(id, target, fromUnix, toUnix, maxDataPoints, consolidator)
		reqs[i] = req
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

	out, err := getTargets(store, reqs)
	if err != nil {
		log.Error(0, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
	if time.Since(startupTime) < warmupPeriod {
		http.Error(w, "Service not ready", http.StatusServiceUnavailable)
		return
	}

	w.Write([]byte("OK"))
	return
}
