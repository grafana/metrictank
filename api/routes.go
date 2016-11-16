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
	// Internal api endpoints used for inter cluster communication
	r.Group("/cluster", func() {
		r.Get("/", s.getClusterStatus)
		r.Combo("/getdata", bind(models.GetData{})).Get(s.getData).Post(s.getData)
		r.Combo("/index/find", bind(models.IndexFind{})).Get(s.indexFind).Post(s.indexFind)
		r.Combo("/index/get", bind(models.IndexGet{})).Get(s.indexGet).Post(s.indexGet)
		r.Combo("/index/list", bind(models.IndexList{})).Get(s.indexList).Post(s.indexList)
	})

	r.Options("/*", func(ctx *macaron.Context) {
		ctx.Write(nil)
	})

	// Graphite endpoints
	r.Combo("/render", withOrg, bind(models.GraphiteRender{})).Get(s.renderMetrics).Post(s.renderMetrics)
	r.Combo("/metrics/find", withOrg, bind(models.GraphiteFind{})).Get(s.metricsFind).Post(s.metricsFind)
	r.Get("/metrics/index.json", withOrg, s.metricsIndex)
	r.Post("/metrics/delete", withOrg, bind(models.MetricsDelete{}), s.metricsDelete)

}
