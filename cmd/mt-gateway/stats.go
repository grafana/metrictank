package main

import (
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/stats"
)

//http.ResponseWriter that saves the status code and body size
type responseRecorder struct {
	http.ResponseWriter
	status int
	size   int
}

//delegate to the main response writer, but save the code
func (rec *responseRecorder) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

//delegate to the main response writer, but record the number of bytes written
func (rec *responseRecorder) Write(data []byte) (int, error) {
	size, err := rec.ResponseWriter.Write(data)
	rec.size += size
	return size, err
}

type requestStats struct {
	sync.Mutex
	responseCounts    map[string]map[int]*stats.CounterRate32
	latencyHistograms map[string]*stats.LatencyHistogram15s32
	sizeMeters        map[string]*stats.Meter32
}

func (r *requestStats) PathStatusCount(path string, status int) {
	metricKey := fmt.Sprintf("api.request.%s.status.%d", path, status)
	r.Lock()
	p, ok := r.responseCounts[path]
	if !ok {
		p = make(map[int]*stats.CounterRate32)
		r.responseCounts[path] = p
	}
	c, ok := p[status]
	if !ok {
		c = stats.NewCounterRate32(metricKey)
		p[status] = c
	}
	r.Unlock()
	c.Inc()
}

func (r *requestStats) PathLatency(path string, dur time.Duration) {
	r.Lock()
	p, ok := r.latencyHistograms[path]
	if !ok {
		p = stats.NewLatencyHistogram15s32(fmt.Sprintf("api.request.%s", path))
		r.latencyHistograms[path] = p
	}
	r.Unlock()
	p.Value(dur)
}

func (r *requestStats) PathSize(path string, size int) {
	r.Lock()
	p, ok := r.sizeMeters[path]
	if !ok {
		p = stats.NewMeter32(fmt.Sprintf("api.request.%s.size", path), false)
		r.sizeMeters[path] = p
	}
	r.Unlock()
	p.Value(size)
}

//add request metrics to the given handler
func statsMiddleware(base http.Handler) http.Handler {
	stats := requestStats{
		responseCounts:    make(map[string]map[int]*stats.CounterRate32),
		latencyHistograms: make(map[string]*stats.LatencyHistogram15s32),
		sizeMeters:        make(map[string]*stats.Meter32),
	}

	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		start := time.Now()
		recorder := responseRecorder{w, -1, 0}
		base.ServeHTTP(&recorder, request)
		path := pathSlug(request.URL.Path)
		stats.PathLatency(path, time.Since(start))
		stats.PathStatusCount(path, recorder.status)
		// only record the request size if the request succeeded.
		if recorder.status < 300 {
			stats.PathSize(path, recorder.size)
		}
	})
}

//convert the request path to a metrics-safe slug
func pathSlug(p string) string {
	slug := strings.TrimPrefix(path.Clean(p), "/")
	if slug == "" {
		slug = "root"
	}
	return strings.Replace(strings.Replace(slug, "/", "_", -1), ".", "_", -1)
}
