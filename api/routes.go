package api

import (
	"github.com/go-macaron/binding"
	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/raintank/gziper"
	"gopkg.in/macaron.v1"
)

func (s *Server) RegisterRoutes() {
	r := s.Macaron
	r.Use(middleware.RequestStats())
	r.Use(middleware.Tracer(s.Tracer))
	r.Use(middleware.OrgMiddleware(multiTenant))
	r.Use(middleware.Logger())
	if useGzip {
		r.Use(gziper.Gziper())
	}
	r.Use(macaron.Renderer())
	r.Use(middleware.CorsHandler())
	bind := binding.Bind
	withOrg := middleware.RequireOrg()
	cBody := middleware.CaptureBody
	ready := middleware.NodeReady()
	noTrace := middleware.DisableTracing

	r.Get("/", noTrace, s.appStatus)
	r.Get("/node", noTrace, s.getNodeStatus)
	r.Post("/node", bind(models.NodeStatus{}), s.setNodeStatus)
	r.Get("/priority", s.explainPriority)
	r.Get("/debug/pprof/block", blockHandler)
	r.Get("/debug/pprof/mutex", mutexHandler)

	r.Get("/cluster", s.getClusterStatus)
	r.Post("/cluster", bind(models.ClusterMembers{}), s.postClusterMembers)

	r.Combo("/getdata", ready, bind(models.GetData{})).Get(s.getData).Post(s.getData)

	// Intra-cluster (inter-node) communication
	r.Combo("/index/find", ready, bind(models.IndexFind{})).Get(s.indexFind).Post(s.indexFind)
	r.Combo("/index/list", ready, bind(models.IndexList{})).Get(s.indexList).Post(s.indexList)
	r.Combo("/index/delete", ready, bind(models.IndexDelete{})).Get(s.indexDelete).Post(s.indexDelete)
	r.Combo("/index/get", ready, bind(models.IndexGet{})).Get(s.indexGet).Post(s.indexGet)
	r.Combo("/index/find_by_tag", ready, bind(models.IndexFindByTag{})).Get(s.indexFindByTag).Post(s.indexFindByTag)
	r.Combo("/index/tags", ready, bind(models.IndexTags{})).Get(s.indexTags).Post(s.indexTags)
	r.Combo("/index/tag_details", ready, bind(models.IndexTagDetails{})).Get(s.indexTagDetails).Post(s.indexTagDetails)
	r.Combo("/index/tags/autoComplete/tags", ready, bind(models.IndexAutoCompleteTags{})).Get(s.indexAutoCompleteTags).Post(s.indexAutoCompleteTags)
	r.Combo("/index/tags/autoComplete/values", ready, bind(models.IndexAutoCompleteTagValues{})).Get(s.indexAutoCompleteTagValues).Post(s.indexAutoCompleteTagValues)
	r.Combo("/index/tags/delSeries", ready, bind(models.IndexTagDelSeries{})).Get(s.indexTagDelSeries).Post(s.indexTagDelSeries)
	r.Combo("/index/tags/terms", ready, bind(models.IndexTagTerms{})).Get(s.IndexTagTerms).Post(s.IndexTagTerms)

	r.Options("/*", func(ctx *macaron.Context) {
		ctx.Write(nil)
	})

	// Miscellaneous Metrictank-only user facing endpoints
	r.Combo("/showplan", cBody, withOrg, ready, bind(models.GraphiteRender{})).Get(s.showPlan).Post(s.showPlan)
	r.Combo("/tags/terms", ready, bind(models.GraphiteTagTerms{})).Get(s.graphiteTagTerms).Post(s.graphiteTagTerms)
	r.Combo("/ccache/delete", bind(models.CCacheDelete{})).Post(s.ccacheDelete).Get(s.ccacheDelete)

	// Graphite endpoints
	r.Combo("/render", cBody, withOrg, ready, bind(models.GraphiteRender{})).Get(s.renderMetrics).Post(s.renderMetrics)
	r.Combo("/metrics/find", withOrg, ready, bind(models.GraphiteFind{})).Get(s.metricsFind).Post(s.metricsFind)
	r.Get("/metrics/index.json", withOrg, ready, s.metricsIndex)
	r.Post("/metrics/delete", withOrg, ready, bind(models.MetricsDelete{}), s.metricsDelete)
	r.Combo("/tags/findSeries", withOrg, ready, bind(models.GraphiteTagFindSeries{})).Get(s.graphiteTagFindSeries).Post(s.graphiteTagFindSeries)
	r.Combo("/tags", withOrg, ready, bind(models.GraphiteTags{})).Get(s.graphiteTags).Post(s.graphiteTags)
	r.Combo("/tags/:tag([0-9a-zA-Z]+)", withOrg, ready, bind(models.GraphiteTagDetails{})).Get(s.graphiteTagDetails).Post(s.graphiteTagDetails)
	r.Combo("/tags/autoComplete/tags", withOrg, ready, bind(models.GraphiteAutoCompleteTags{})).Get(s.graphiteAutoCompleteTags).Post(s.graphiteAutoCompleteTags)
	r.Combo("/tags/autoComplete/values", withOrg, ready, bind(models.GraphiteAutoCompleteTagValues{})).Get(s.graphiteAutoCompleteTagValues).Post(s.graphiteAutoCompleteTagValues)
	r.Post("/tags/delSeries", withOrg, ready, bind(models.GraphiteTagDelSeries{}), s.graphiteTagDelSeries)
	r.Combo("/functions", withOrg).Get(s.graphiteFunctions).Post(s.graphiteFunctions)
	r.Combo("/functions/:func(.+)", withOrg).Get(s.graphiteFunctions).Post(s.graphiteFunctions)

	// Meta Tags
	r.Post("/metaTags/upsert", withOrg, ready, bind(models.MetaTagRecordUpsert{}), s.metaTagRecordUpsert)
	r.Post("/metaTags/swap", withOrg, ready, bind(models.MetaTagRecordSwap{}), s.metaTagRecordSwap)
	r.Get("/metaTags", withOrg, ready, s.getMetaTagRecords)

	// Prometheus metrics endpoint
	r.Get("/prometheus/metrics", promhttp.Handler())
}
