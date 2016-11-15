package api

import (
	"github.com/go-macaron/binding"
	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
	"gopkg.in/macaron.v1"
)

func (s *Server) RegisterRoutes() {
	r := s.Macaron
	r.Use(macaron.Renderer())
	r.Use(middleware.OrgMiddleware())
	r.Use(middleware.CorsHandler())

	bind := binding.Bind
	withOrg := middleware.RequireOrg()

	r.Get("/", s.appStatus)
	r.Get("/node", s.getNodeStatus)
	r.Post("/node", bind(models.NodeStatus{}), s.setNodeStatus)

	r.Options("/*", func(ctx *macaron.Context) {
		ctx.Write(nil)
	})

	// Graphite endpoints
	r.Combo("/render", withOrg, bind(models.GraphiteRender{})).Get(s.renderMetrics).Post(s.renderMetrics)
	r.Combo("/metrics/find", withOrg, bind(models.GraphiteFind{})).Get(s.metricsFind).Post(s.metricsFind)
	r.Get("/metrics/index.json", withOrg, s.metricsIndex)
	r.Post("/metrics/delete", withOrg, bind(models.MetricsDelete{}), s.metricsDelete)

}
