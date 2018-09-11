package middleware

import (
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/stats"
	"gopkg.in/macaron.v1"
)

type requestStats struct {
	sync.Mutex
	responseCounts    map[string]map[int]*stats.Counter32
	latencyHistograms map[string]*stats.LatencyHistogram15s32
	sizeMeters        map[string]*stats.Meter32
}

func (r *requestStats) PathStatusCount(path string, status int) {
	r.Lock()
	p, ok := r.responseCounts[path]
	if !ok {
		p = make(map[int]*stats.Counter32)
		r.responseCounts[path] = p
	}
	c, ok := p[status]
	if !ok {
		// metric api.request.%s.status.%d is the count of the number of responses for each request path, status code combination.
		// eg. `api.requests.metrics_find.status.200` and `api.request.render.status.503`
		c = stats.NewCounter32(fmt.Sprintf("api.request.%s.status.%d", path, status))
		p[status] = c
	}
	r.Unlock()
	c.Inc()
}

func (r *requestStats) PathLatency(path string, dur time.Duration) {
	r.Lock()
	p, ok := r.latencyHistograms[path]
	if !ok {
		// metric api.request.%s is the latency of each request by request path.
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
		// metric api.request.%s.size is the size of each response by request path
		p = stats.NewMeter32(fmt.Sprintf("api.request.%s.size", path), false)
		r.sizeMeters[path] = p
	}
	r.Unlock()
	p.Value(size)
}

// RequestStats returns a middleware that tracks request metrics.
func RequestStats() macaron.Handler {
	stats := requestStats{
		responseCounts:    make(map[string]map[int]*stats.Counter32),
		latencyHistograms: make(map[string]*stats.LatencyHistogram15s32),
		sizeMeters:        make(map[string]*stats.Meter32),
	}

	return func(ctx *macaron.Context) {
		start := time.Now()
		rw := ctx.Resp.(macaron.ResponseWriter)
		// call next handler. This will return after all handlers
		// have completed and the request has been sent.
		ctx.Next()
		status := rw.Status()
		path := pathSlug(ctx.Req.URL.Path)
		// graphite cluster requests use local=1
		// this way we can differentiate "full" render requests from client to MT (encompassing data processing, proxing to graphite, etc)
		// from "subrequests" where metrictank is called by graphite and graphite does the processing and returns to the client
		if ctx.Req.Request.Form.Get("local") == "1" {
			path += "-local"
		}
		stats.PathStatusCount(path, status)
		stats.PathLatency(path, time.Since(start))
		// only record the request size if the request succeeded.
		if status < 300 {
			stats.PathSize(path, rw.Size())
		}
	}
}

func pathSlug(p string) string {
	slug := strings.TrimPrefix(path.Clean(p), "/")
	if slug == "" {
		slug = "root"
	}
	return strings.Replace(slug, "/", "_", -1)
}
