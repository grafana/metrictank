package api

import (
	"github.com/go-macaron/binding"
	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/raintank/gziper"
	macaron "gopkg.in/macaron.v1"
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
	form := binding.Form
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

	r.Combo("/index/find", ready, bind(models.IndexFind{})).Get(s.indexFind).Post(s.indexFind)
	r.Combo("/index/list", ready, bind(models.IndexList{})).Get(s.indexList).Post(s.indexList)
	r.Combo("/index/delete", ready, bind(models.IndexDelete{})).Get(s.indexDelete).Post(s.indexDelete)
	r.Combo("/index/get", ready, bind(models.IndexGet{})).Get(s.indexGet).Post(s.indexGet)
	r.Combo("/index/tags", ready, bind(models.IndexTags{})).Get(s.indexTags).Post(s.indexTags)
	r.Combo("/index/find_by_tag", ready, bind(models.IndexFindByTag{})).Get(s.indexFindByTag).Post(s.indexFindByTag)
	r.Combo("/index/tag_details", ready, bind(models.IndexTagDetails{})).Get(s.indexTagDetails).Post(s.indexTagDetails)
	r.Combo("/index/tags/autoComplete/tags", ready, bind(models.IndexAutoCompleteTags{})).Get(s.indexAutoCompleteTags).Post(s.indexAutoCompleteTags)
	r.Combo("/index/tags/autoComplete/values", ready, bind(models.IndexAutoCompleteTagValues{})).Get(s.indexAutoCompleteTagValues).Post(s.indexAutoCompleteTagValues)
	r.Combo("/index/tags/delSeries", ready, bind(models.IndexTagDelSeries{})).Get(s.indexTagDelSeries).Post(s.indexTagDelSeries)

	r.Combo("/ccache/delete", bind(models.CCacheDelete{})).Post(s.ccacheDelete).Get(s.ccacheDelete)

	r.Options("/*", func(ctx *macaron.Context) {
		ctx.Write(nil)
	})

	r.Combo("/showplan", cBody, withOrg, ready, bind(models.GraphiteRender{})).Get(s.showPlan).Post(s.showPlan)

	// Graphite endpoints
	r.Combo("/render", cBody, withOrg, ready, bind(models.GraphiteRender{})).Get(s.renderMetrics).Post(s.renderMetrics)
	r.Combo("/metrics/find", withOrg, ready, bind(models.GraphiteFind{})).Get(s.metricsFind).Post(s.metricsFind)
	r.Get("/metrics/index.json", withOrg, ready, s.metricsIndex)
	r.Post("/metrics/delete", withOrg, ready, bind(models.MetricsDelete{}), s.metricsDelete)
	r.Combo("/tags", withOrg, ready, bind(models.GraphiteTags{})).Get(s.graphiteTags).Post(s.graphiteTags)
	r.Combo("/tags/:tag([0-9a-zA-Z]+)", withOrg, ready, bind(models.GraphiteTagDetails{})).Get(s.graphiteTagDetails).Post(s.graphiteTagDetails)
	r.Combo("/tags/findSeries", withOrg, ready, bind(models.GraphiteTagFindSeries{})).Get(s.graphiteTagFindSeries).Post(s.graphiteTagFindSeries)
	r.Combo("/tags/autoComplete/tags", withOrg, ready, bind(models.GraphiteAutoCompleteTags{})).Get(s.graphiteAutoCompleteTags).Post(s.graphiteAutoCompleteTags)
	r.Combo("/tags/autoComplete/values", withOrg, ready, bind(models.GraphiteAutoCompleteTagValues{})).Get(s.graphiteAutoCompleteTagValues).Post(s.graphiteAutoCompleteTagValues)
	r.Post("/tags/delSeries", withOrg, ready, bind(models.GraphiteTagDelSeries{}), s.graphiteTagDelSeries)
	r.Combo("/functions", withOrg).Get(s.graphiteFunctions).Post(s.graphiteFunctions)
	r.Combo("/functions/:func(.+)", withOrg).Get(s.graphiteFunctions).Post(s.graphiteFunctions)

	// Meta Tags
	r.Post("/metaTags/add", withOrg, ready, bind(models.MetaTagRecord{}), s.metaTagRecordUpsert)
	r.Get("/metaTags", withOrg, ready, s.getMetaTagRecord)

	// Prometheus endpoints
	r.Combo("/prometheus/api/v1/query_range", cBody, withOrg, ready, form(models.PrometheusRangeQuery{})).Get(s.prometheusQueryRange).Post(s.prometheusQueryRange)
	r.Combo("/prometheus/api/v1/query", cBody, withOrg, ready, form(models.PrometheusQueryInstant{})).Get(s.prometheusQueryInstant).Post(s.prometheusQueryInstant)
	r.Combo("/prometheus/api/v1/series", cBody, withOrg, ready, form(models.PrometheusSeriesQuery{})).Get(s.prometheusQuerySeries).Post(s.prometheusQuerySeries)
	r.Get("/prometheus/api/v1/label/:name/values", cBody, withOrg, ready, s.prometheusLabelValues)
	r.Get("/prometheus/metrics", promhttp.Handler())
}
