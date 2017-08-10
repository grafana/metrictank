package middleware

import (
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"gopkg.in/macaron.v1"
)

// Tracer returns a middleware that traces requests
func Tracer(tracer opentracing.Tracer) macaron.Handler {
	return func(macCtx *macaron.Context) {

		path := pathSlug(macCtx.Req.URL.Path)
		// graphite cluster requests use local=1
		// this way we can differentiate "full" render requests from client to MT (encompassing data processing, proxing to graphite, etc)
		// from "subrequests" where metrictank is called by graphite and graphite does the processing and returns to the client
		if macCtx.Req.Request.Form.Get("local") == "1" {
			path += "-local"
		}

		spanCtx, _ := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(macCtx.Req.Header))
		sp := tracer.StartSpan("HTTP "+macCtx.Req.Method+" "+path, ext.RPCServerOption(spanCtx))

		ext.HTTPMethod.Set(sp, macCtx.Req.Method)
		ext.HTTPUrl.Set(sp, macCtx.Req.URL.String())
		ext.Component.Set(sp, "metrictank/api")

		macCtx.Req = macaron.Request{macCtx.Req.WithContext(opentracing.ContextWithSpan(macCtx.Req.Context(), sp))}

		rw := macCtx.Resp.(macaron.ResponseWriter)
		// call next handler. This will return after all handlers
		// have completed and the request has been sent.
		macCtx.Next()
		status := rw.Status()
		ext.HTTPStatusCode.Set(sp, uint16(status))
		if status >= 200 && status < 300 {
			sp.SetTag("http.size", rw.Size())
		}
		// TODO: else write error msg?
		sp.Finish()
	}
}
