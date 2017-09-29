package middleware

import (
	"github.com/grafana/metrictank/cluster"
	macaron "gopkg.in/macaron.v1"
)

func NodeReady() macaron.Handler {
	return func(c *Context) {
		if !cluster.Manager.IsReady() {
			c.Error(503, "node not ready")
		}
	}
}
