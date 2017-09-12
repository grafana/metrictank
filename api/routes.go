package api

import (
	"github.com/go-macaron/binding"
	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/raintank/gziper"
	"gopkg.in/macaron.v1"
)

func (s *Server) RegisterRoutes() {
	r := s.Macaron
	if useGzip {
		r.Use(gziper.Gziper())
	}
	r.Use(middleware.RequestStats())
	r.Use(middleware.Tracer(s.Tracer))
	r.Use(macaron.Renderer())
	r.Use(middleware.OrgMiddleware(multiTenant))
	r.Use(middleware.CorsHandler())

	bind := binding.Bind
	withOrg := middleware.RequireOrg()
	cBody := middleware.CaptureBody
	ready := middleware.NodeReady()

	r.Get("/", s.appStatus)
	r.Get("/node", s.getNodeStatus)
	r.Post("/node", bind(models.NodeStatus{}), s.setNodeStatus)
	r.Get("/debug/pprof/block", blockHandler)
	r.Get("/debug/pprof/mutex", mutexHandler)

	r.Get("/cluster", s.getClusterStatus)
	r.Post("/cluster", bind(models.ClusterMembers{}), s.postClusterMembers)

	r.Combo("/getdata", ready, bind(models.GetData{})).Get(s.getData).Post(s.getData)

	r.Combo("/index/find", ready, bind(models.IndexFind{})).Get(s.indexFind).Post(s.indexFind)
	r.Combo("/index/list", ready, bind(models.IndexList{})).Get(s.indexList).Post(s.indexList)
	r.Combo("/index/delete", ready, bind(models.IndexDelete{})).Get(s.indexDelete).Post(s.indexDelete)
	r.Combo("/index/get", ready, bind(models.IndexGet{})).Get(s.indexGet).Post(s.indexGet)
	r.Combo("/index/tags", ready, bind(models.IndexTagList{})).Get(s.indexTagList).Post(s.indexTagList)
	r.Combo("/index/tags/findSeries", ready, bind(models.IndexTagFindSeries{})).Get(s.indexTagFindSeries).Post(s.indexTagFindSeries)
	r.Combo("/index/tags/:tag([0-9a-zA-Z]+)", ready, bind(models.IndexTag{})).Get(s.indexTag).Post(s.indexTag)

	r.Options("/*", func(ctx *macaron.Context) {
		ctx.Write(nil)
	})

	// Graphite endpoints
	r.Combo("/render", cBody, withOrg, ready, bind(models.GraphiteRender{})).Get(s.renderMetrics).Post(s.renderMetrics)
	r.Combo("/metrics/find", withOrg, ready, bind(models.GraphiteFind{})).Get(s.metricsFind).Post(s.metricsFind)
	r.Get("/metrics/index.json", withOrg, ready, s.metricsIndex)
	r.Post("/metrics/delete", withOrg, ready, bind(models.MetricsDelete{}), s.metricsDelete)
	r.Combo("/tags", withOrg, ready, bind(models.GraphiteTagList{})).Get(s.graphiteTagList).Post(s.graphiteTagList)
	r.Combo("/tags/:tag([0-9a-zA-Z]+)", withOrg, ready, bind(models.GraphiteTag{})).Get(s.graphiteTag).Post(s.graphiteTag)
	r.Combo("/tags/findSeries", withOrg, ready, bind(models.GraphiteTagFindSeries{})).Get(s.graphiteTagFindSeries).Post(s.graphiteTagFindSeries)
}
