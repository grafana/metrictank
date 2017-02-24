package middleware

import (
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/raintank/metrictank/stats"
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
