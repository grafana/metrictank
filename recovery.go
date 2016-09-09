package main

import (
	"net/http"
	"runtime"

	"github.com/raintank/worldping-api/pkg/log"
)

type recoveryHandler struct {
	handler http.Handler
}

// RecoveryHandler is HTTP middleware that recovers from a panic,
// logs the panic as well as details about the request
// and writes http.StatusInternalServerError.
func RecoveryHandler(h http.Handler) http.Handler {
	return &recoveryHandler{handler: h}
}

func (h recoveryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.Error(1, `panic serving %v: %v
%sRequest details:
Method: %s
URL: %s
Headers: %q
Trailer: %q
Body: not available
Formdata: %q
`, req.RemoteAddr, err, buf, req.Method, req.URL, req.Header, req.Trailer, req.Form)

			w.WriteHeader(http.StatusInternalServerError)
		}
	}()

	h.handler.ServeHTTP(w, req)
}
