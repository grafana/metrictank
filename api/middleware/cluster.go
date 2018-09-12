package middleware

import (
	"net/http"

	"github.com/grafana/metrictank/cluster"
	macaron "gopkg.in/macaron.v1"
)

func NodeReady() macaron.Handler {
	return func(c *Context) {
		if !cluster.Manager.IsReady() {
			c.Error(http.StatusServiceUnavailable, "node not ready")
		}
	}
}
