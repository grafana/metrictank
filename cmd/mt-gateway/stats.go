package main



import (
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/stats"
	"github.com/raintank/tsdb-gw/api/models"
	"gopkg.in/macaron.v1"
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

func (r *requestStats) PathStatusCount(ctx *models.Context, path string, status int) {
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

func (r *requestStats) PathLatency(ctx *models.Context, path string, dur time.Duration) {
	r.Lock()
	p, ok := r.latencyHistograms[path]
	if !ok {
		p = stats.NewLatencyHistogram15s32(fmt.Sprintf("api.request.%s", path))
		r.latencyHistograms[path] = p
	}
	r.Unlock()
	p.Value(dur)
}

func (r *requestStats) PathSize(ctx *models.Context, path string, size int) {
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
		responseCounts:    make(map[string]map[int]*stats.CounterRate32),
		latencyHistograms: make(map[string]*stats.LatencyHistogram15s32),
		sizeMeters:        make(map[string]*stats.Meter32),
	}

	return func(ctx *models.Context) {
		start := time.Now()
		rw := ctx.Resp.(macaron.ResponseWriter)
		// call next handler. This will return after all handlers
		// have completed and the request has been sent.
		ctx.Next()
		status := rw.Status()
		path := pathSlug(ctx.Req.URL.Path)
		stats.PathStatusCount(ctx, path, status)
		stats.PathLatency(ctx, path, time.Since(start))
		// only record the request size if the request succeeded.
		if status < 300 {
			stats.PathSize(ctx, path, rw.Size())
		}
	}
}

func pathSlug(p string) string {
	slug := strings.TrimPrefix(path.Clean(p), "/")
	if slug == "" {
		slug = "root"
	}
	return strings.Replace(strings.Replace(slug, "/", "_", -1), ".", "_", -1)
}
