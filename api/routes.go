package api

import (
	"github.com/Unknwon/macaron"
	"github.com/macaron-contrib/binding"
	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
)

func (s *Server) RegisterRoutes() {
	r := s.Macaron
	r.Use(macaron.Renderer())
	r.Use(middleware.OrgMiddleware())
	r.Use(middleware.CorsHandler())

	bind := binding.Bind

	r.Get("/", s.appStatus)
	r.Get("/node", s.getNodeStatus)
	r.Post("/node", bind(models.NodeStatus{}), s.setNodeStatus)
	r.Get("/cluster", s.getClusterStatus)

	/* not sure what even uses this.
	r.Combo("/get").Get(s.getMetrics).Post(s.getMetrics)
	*/

	// Graphite endpoints
	r.Combo("/render", bind(models.GraphiteRender{})).Get(s.renderMetrics).Post(s.renderMetrics)
	r.Combo("/metrics/find", bind(models.GraphiteFind{})).Get(s.metricsFind).Post(s.metricsFind)
	r.Get("/metrics/index.json", s.metricsIndex)
	r.Post("/metrics/delete", bind(models.MetricsDelete{}), s.metricsDelete)

	// Internal api endpoints used for inter cluster communication
	r.Group("/internal", func() {
		r.Combo("/getdata", bind(models.Req{})).Get(s.getData).Post(s.getData)
		r.Combo("/index/find").Get(s.indexFind).Post(s.indexFind)
		r.Combo("/index/get").Get(s.indexGet).Post(s.indexGet)
		r.Combo("/index/list", bind(models.IndexList{})).Get(s.indexList).Post(s.indexList)
	})

}
