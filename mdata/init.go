// Package mdata stands for "managed data" or "metrics data" if you will
// it has all the stuff to keep metric data in memory, store it, and synchronize
// save states over the network
package mdata

import "github.com/raintank/met"

var (
	LogLevel int

	chunkCreate met.Count
	chunkClear  met.Count

	// metric metrics_too_old is points that go back in time.
	// E.g. for any given series, when a point has a timestamp
	// that is not higher than the timestamp of the last written timestamp for that series.
	metricsTooOld met.Count

	// metric add_to_saving_chunk is points received - by the primary node - for the most recent chunk
	// when that chunk is already being saved (or has been saved).
	// this indicates that your GC is actively sealing chunks and saving them before you have the chance to send
	// your (infrequent) updates.  The primary won't add them to its in-memory chunks, but secondaries will
	// (because they are never in "saving" state for them), see below.
	addToSavingChunk met.Count

	// metric add_to_saved_chunk is points received - by a secondary node - for the most recent chunk when that chunk
	// has already been saved by a primary.  A secondary can add this data to its chunks.
	addToSavedChunk met.Count

	memToIterDuration met.Timer
	persistDuration   met.Timer

	metricsActive met.Gauge // metric metrics_active is the amount of currently known metrics (excl rollup series), measured every second
	gcMetric      met.Count // metric gc_metric is the amount of times the metrics GC is about to inspect a metric (series)
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
