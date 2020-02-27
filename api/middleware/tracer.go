package middleware

import (
	"context"
	"errors"
	"math/rand"
	"net/http"

	"github.com/grafana/metrictank/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	jaeger "github.com/uber/jaeger-client-go"
	"gopkg.in/macaron.v1"
)

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

func DisableTracing(c *Context) {
	c.Data["noTrace"] = true
}

// key used for adding traceID to a context.Context
type TraceID struct{}

// Tracer returns a middleware that traces requests
func Tracer(tracer opentracing.Tracer) macaron.Handler {
	return func(macCtx *macaron.Context) {
		path := pathSlug(macCtx.Req.URL.Path)
		// graphite cluster requests use local=1
		// this way we can differentiate "full" render requests from client to MT (encompassing data processing, proxying to graphite, etc)
		// from "subrequests" where metrictank is called by graphite and graphite does the processing and returns to the client
		if macCtx.Req.Request.Form.Get("local") == "1" {
			path += "-local"
		}

		spanCtx, _ := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(macCtx.Req.Header))
		span := tracer.StartSpan("HTTP "+macCtx.Req.Method+" "+path, ext.RPCServerOption(spanCtx))

		ext.HTTPMethod.Set(span, macCtx.Req.Method)
		ext.HTTPUrl.Set(span, macCtx.Req.URL.String())
		ext.Component.Set(span, "metrictank/api")

		macCtx.Resp = &TracingResponseWriter{
			ResponseWriter: macCtx.Resp,
		}
		macCtx.MapTo(macCtx.Resp, (*http.ResponseWriter)(nil))

		rw := macCtx.Resp.(*TracingResponseWriter)

		var traceID string
		// if tracing is enabled (context is not a opentracing.noopSpanContext)
		// use the existing traceID and store the traceID in output headers
		if spanCtx, ok := span.Context().(jaeger.SpanContext); ok {
			traceID = spanCtx.TraceID().String()
			macCtx.Resp.Header().Set("Trace-Id", traceID)
		} else {
			// no traceId, so lets generate a new one.
			traceID = jaeger.TraceID{Low: uint64(rand.Int63())}.String()
		}

		// update the context.Context in our http.Request to include our openTracing span
		// and also just store the traceID in the context for easy retrival.
		macCtx.Req = macaron.Request{
			Request: macCtx.Req.WithContext(context.WithValue(opentracing.ContextWithSpan(macCtx.Req.Context(), span), TraceID{}, traceID)),
		}

		// call next handler. This will return after all handlers
		// have completed and the request has been sent.
		macCtx.Next()

		// if tracing has been disabled we return directly without calling
		// span.Finish()
		if noTrace, ok := macCtx.Data["noTrace"]; ok && noTrace.(bool) {
			return
		}

		status := rw.Status()
		ext.HTTPStatusCode.Set(span, uint16(status))
		if status >= 200 && status < 300 {
			span.SetTag("http.size", rw.Size())
		}
		if status >= 400 {
			tracing.Error(span, errors.New(string(rw.errBody)))
			if status >= http.StatusInternalServerError {
				tracing.Failure(span)
			}
		}
		span.Finish()
	}
}
