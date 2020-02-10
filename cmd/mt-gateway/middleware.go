package main



import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/stats"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	otLog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/raintank/tsdb-gw/api/models"
	"github.com/raintank/tsdb-gw/auth"
	"github.com/raintank/tsdb-gw/ingest"
	log "github.com/sirupsen/logrus"
	"gopkg.in/macaron.v1"
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tsdb_gw",
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",
		// github.com/weaveworks/common/instrument/instrument.DefBuckets
		// Pulling it in brings a ton of dependencies.
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 100},
	}, []string{"method", "route", "status_code"})
)

func init() {
	prometheus.MustRegister(requestDuration)
}

type TracingResponseWriter struct {
	macaron.ResponseWriter
	errBody []byte // the body in case it is an error
}

func (rw *TracingResponseWriter) Write(b []byte) (int, error) {
	if rw.ResponseWriter.Status() >= 400 {
		rw.errBody = make([]byte, len(b))
		copy(rw.errBody, b)
	}
	return rw.ResponseWriter.Write(b)
}

func (rw *TracingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("the ResponseWriter doesn't support the Hijacker interface")
	}
	return hijacker.Hijack()
}

func GetContextHandler() macaron.Handler {
	return func(c *macaron.Context) {
		ctx := &models.Context{
			Context: c,
			User:    &auth.User{},
		}
		c.Map(ctx)
	}
}

func RequireAdmin() macaron.Handler {
	return func(ctx *models.Context) {
		if !ctx.IsAdmin {
			log.Infof("HTTP auth: admin required but user %d is not admin -> 403", ctx.ID)
			ctx.JSON(403, "Permision denied")
		}
	}
}

func RequirePublisher() macaron.Handler {
	return func(ctx *models.Context) {
		if !ctx.Role.IsPublisher() {
			log.Infof("HTTP auth: user %v with role %v attempting to publish at %v -> 401", ctx.ID, ctx.Role, ctx.Req.RequestURI)
			ctx.JSON(401, "Unauthorized to publish")
			return
		}
	}
}

func RequireViewer() macaron.Handler {
	return func(ctx *models.Context) {
		if !ctx.Role.IsViewer() {
			log.Infof("HTTP auth: user %v with role %v attempting to view at %v -> 403", ctx.ID, ctx.Role, ctx.Req.RequestURI)
			ctx.JSON(403, "Unauthorized to view")
			return
		}
	}
}

func IngestRateLimiter() macaron.Handler {
	return func(ctx *models.Context) {
		if !ingest.IsRateBudgetAvailable(ctx.Req.Context(), ctx.ID) {
			log.Infof("HTTP ratelimiter: Rejecting request for %d due to rate limit -> 429", ctx.ID)
			ctx.JSON(http.StatusTooManyRequests, "Rate limit is exhausted")
			return
		}
	}
}

func (a *Api) GenerateHandlers(kind string, enforceRoles, datadog, rateLimit bool, handlers ...macaron.Handler) []macaron.Handler {
	combinedHandlers := []macaron.Handler{}
	if kind == "write" {
		if datadog {
			combinedHandlers = append(combinedHandlers, a.DDAuth())
		} else {
			combinedHandlers = append(combinedHandlers, a.Auth())
		}
		if enforceRoles {
			combinedHandlers = append(combinedHandlers, RequirePublisher())
		}
	} else {
		combinedHandlers = append(combinedHandlers, a.Auth())
		if enforceRoles {
			combinedHandlers = append(combinedHandlers, RequirePublisher())
		}
	}

	if ingest.UseRateLimit() && rateLimit {
		combinedHandlers = append(combinedHandlers, IngestRateLimiter())
	}

	return append(combinedHandlers, handlers...)
}

func getAuthCreds(req *http.Request) (user, password string) {
	username, key, ok := req.BasicAuth()
	if !ok {
		// no basicAuth, but we also need to check for a Bearer Token
		header := req.Header.Get("Authorization")
		parts := strings.SplitN(header, " ", 2)
		if len(parts) == 2 && parts[0] == "Bearer" {
			keyParts := strings.SplitN(parts[1], ":", 2)
			if len(keyParts) < 2 {
				key = keyParts[0]
				username = "api_key"
			} else {
				key = keyParts[1]
				username = keyParts[0]
			}
		}
	}
	return username, key
}

func (a *Api) Auth() macaron.Handler {
	return func(ctx *models.Context) {
		username, key := getAuthCreds(ctx.Req.Request)
		if key == "" {
			log.Debugf("HTTP auth: no key specified -> 401")
			ctx.JSON(401, "Unauthorized")
			return
		}

		user, err := a.authPlugin.Auth(username, key)
		if err != nil {
			if err == auth.ErrInvalidCredentials || err == auth.ErrInvalidOrgId || err == auth.ErrInvalidInstanceID {
				ctx.JSON(401, err.Error())
				return
			}
			log.Errorf("HTTP auth: failed to perform authentication: %q -> 500", err.Error())
			ctx.JSON(500, err.Error())
			return
		}

		// allow admin users to impersonate other orgs.
		if user.IsAdmin {
			header := ctx.Req.Header.Get("X-Tsdb-Org")
			if header != "" {
				orgId, err := strconv.ParseInt(header, 10, 64)
				if err == nil && orgId != 0 {
					user.ID = int(orgId)
				}
			}
		}
		ctx.User = user
	}
}

func (a *Api) DDAuth() macaron.Handler {
	return func(ctx *models.Context) {
		var key string
		var username string

		header := ctx.Req.Header.Get("Dd-Api-Key")
		parts := strings.SplitN(header, ":", 2)
		if len(parts) == 1 {
			key = parts[0]
			username = "api_key"
		} else if len(parts) == 2 && parts[1] == "" {
			key = parts[1]
			username = "api_key"
		} else {
			key = parts[1]
			username = parts[0]
		}

		if key == "" {
			log.Debugf("HTTP DDauth: no key specified -> 401")
			ctx.JSON(401, "Unauthorized")
			return
		}

		user, err := a.authPlugin.Auth(username, key)
		if err != nil {
			if err == auth.ErrInvalidCredentials || err == auth.ErrInvalidOrgId || err == auth.ErrInvalidInstanceID {
				ctx.JSON(401, err.Error())
				return
			}
			log.Errorf("HTTP DDauth: failed to perform authentication: %q -> 500", err.Error())
			ctx.JSON(500, err.Error())
			return
		}

		// allow admin users to impersonate other orgs.
		if user.IsAdmin {
			header := ctx.Req.Header.Get("X-Tsdb-Org")
			if header != "" {
				orgId, err := strconv.ParseInt(header, 10, 64)
				if err == nil && orgId != 0 {
					user.ID = int(orgId)
				}
			}
		}
		ctx.User = user
	}
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

func (a *Api) PromStats(handler string) macaron.Handler {
	return func(ctx *models.Context) {
		start := time.Now()
		rw := ctx.Resp.(macaron.ResponseWriter)
		// call next handler. This will return after all handlers
		// have completed and the request has been sent.
		ctx.Next()

		status := strconv.Itoa(rw.Status())
		took := time.Since(start)
		method := ctx.Req.Method

		requestDuration.WithLabelValues(method, handler, status).Observe(took.Seconds())
	}
}

func pathSlug(p string) string {
	slug := strings.TrimPrefix(path.Clean(p), "/")
	if slug == "" {
		slug = "root"
	}
	return strings.Replace(strings.Replace(slug, "/", "_", -1), ".", "_", -1)
}

func Tracer(componentName string) macaron.Handler {
	return func(macCtx *macaron.Context) {
		tracer := opentracing.GlobalTracer()
		path := pathSlug(macCtx.Req.URL.Path)
		spanCtx, _ := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(macCtx.Req.Header))
		span := tracer.StartSpan("HTTP "+macCtx.Req.Method+" "+path, ext.RPCServerOption(spanCtx))

		ext.HTTPMethod.Set(span, macCtx.Req.Method)
		ext.HTTPUrl.Set(span, macCtx.Req.URL.String())
		ext.Component.Set(span, componentName+"/api")

		macCtx.Req = macaron.Request{
			Request: macCtx.Req.WithContext(opentracing.ContextWithSpan(macCtx.Req.Context(), span)),
		}
		macCtx.Resp = &TracingResponseWriter{
			ResponseWriter: macCtx.Resp,
		}
		macCtx.MapTo(macCtx.Resp, (*http.ResponseWriter)(nil))

		rw := macCtx.Resp.(*TracingResponseWriter)
		// call next handler. This will return after all handlers
		// have completed and the request has been sent.
		macCtx.Next()
		status := rw.Status()
		ext.HTTPStatusCode.Set(span, uint16(status))
		if status >= 200 && status < 300 {
			span.SetTag("http.size", rw.Size())
		}
		if status >= 400 {
			span.LogFields(otLog.Error(errors.New(string(rw.errBody))))
			if status >= 500 {
				ext.Error.Set(span, true)
			}
		}
		span.Finish()
	}
}

func CaptureBody(c *models.Context) {
	body, err := ioutil.ReadAll(c.Req.Request.Body)
	if err != nil {
		log.Error(3, "HTTP internal error: failed to read request body for proxying: %s -> 500", err)
		c.PlainText(500, []byte("internal error: failed to read request body for proxying"))
	}
	c.Req.Request.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	c.Body = ioutil.NopCloser(bytes.NewBuffer(body))
}