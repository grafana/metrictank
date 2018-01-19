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
	r.Combo("/index/tags", ready, bind(models.IndexTags{})).Get(s.indexTags).Post(s.indexTags)
	r.Combo("/index/find_by_tag", ready, bind(models.IndexFindByTag{})).Get(s.indexFindByTag).Post(s.indexFindByTag)
	r.Combo("/index/tag_details", ready, bind(models.IndexTagDetails{})).Get(s.indexTagDetails).Post(s.indexTagDetails)
	r.Combo("/index/tags/autoComplete/tags", ready, bind(models.IndexAutoCompleteTags{})).Get(s.indexAutoCompleteTags).Post(s.indexAutoCompleteTags)
	r.Combo("/index/tags/autoComplete/values", ready, bind(models.IndexAutoCompleteTags{})).Get(s.indexAutoCompleteTagValues).Post(s.indexAutoCompleteTagValues)

	r.Options("/*", func(ctx *macaron.Context) {
		ctx.Write(nil)
	})

	// Graphite endpoints
	r.Combo("/render", cBody, withOrg, ready, bind(models.GraphiteRender{})).Get(s.renderMetrics).Post(s.renderMetrics)
	r.Combo("/metrics/find", withOrg, ready, bind(models.GraphiteFind{})).Get(s.metricsFind).Post(s.metricsFind)
	r.Get("/metrics/index.json", withOrg, ready, s.metricsIndex)
	r.Post("/metrics/delete", withOrg, ready, bind(models.MetricsDelete{}), s.metricsDelete)
	r.Combo("/metrics/tags", withOrg, ready, bind(models.GraphiteTags{})).Get(s.graphiteTags).Post(s.graphiteTags)
	r.Combo("/metrics/tags/:tag([0-9a-zA-Z]+)", withOrg, ready, bind(models.GraphiteTagDetails{})).Get(s.graphiteTagDetails).Post(s.graphiteTagDetails)
	r.Combo("/metrics/tags/findSeries", withOrg, ready, bind(models.GraphiteTagFindSeries{})).Get(s.graphiteTagFindSeries).Post(s.graphiteTagFindSeries)
	r.Combo("/metrics/tags/autoComplete/tags", withOrg, ready, bind(models.GraphiteAutoCompleteTags{})).Get(s.graphiteAutoCompleteTags).Post(s.graphiteAutoCompleteTags)
	//r.Combo("/metrics/tags/autoComplete/values", withOrg, ready, bind(models.GraphiteAutoCompleteValues{})).Get(s.graphiteAutoCompleteValues).Post(s.graphiteAutoCompleteValues)
}
