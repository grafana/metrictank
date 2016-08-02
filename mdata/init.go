// Package mdata stands for "managed data" or "metrics data" if you will
// it has all the stuff to keep metric data in memory, store it, and synchronize
// save states over the network
package mdata

import "github.com/raintank/met"

var (
	LogLevel int

	chunkCreate met.Count
	chunkClear  met.Count

	metricsTooOld    met.Count
	addToSavingChunk met.Count
	addToSavedChunk  met.Count

	memToIterDuration met.Timer
	persistDuration   met.Timer

	metricsActive met.Gauge
	gcMetric      met.Count // metrics GC

	OffsetFence *int64 // when using kafka for metrics and clustering, points to the offset of a kafka metrics stream to guarantee some synchronisation between cluster and metrics feeds
)

func InitMetrics(stats met.Backend) {
	chunkCreate = stats.NewCount("chunks.create")
	chunkClear = stats.NewCount("chunks.clear")

	metricsTooOld = stats.NewCount("metrics_too_old")
	addToSavingChunk = stats.NewCount("add_to_saving_chunk")
	addToSavedChunk = stats.NewCount("add_to_saved_chunk")

	memToIterDuration = stats.NewTimer("mem.to_iter_duration", 0)
	persistDuration = stats.NewTimer("persist_duration", 0)

	gcMetric = stats.NewCount("gc_metric")
	metricsActive = stats.NewGauge("metrics_active", 0)
}
