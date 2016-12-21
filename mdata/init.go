// Package mdata stands for "managed data" or "metrics data" if you will
// it has all the stuff to keep metric data in memory, store it, and synchronize
// save states over the network
package mdata

import "github.com/raintank/metrictank/stats"

var (
	LogLevel int

	chunkCreate = stats.NewCounter32("tank.chunk_operations.create")
	chunkClear  = stats.NewCounter32("tank.chunk_operations.clear")

	// metric metrics_too_old is points that go back in time.
	// E.g. for any given series, when a point has a timestamp
	// that is not higher than the timestamp of the last written timestamp for that series.
	metricsTooOld = stats.NewCounter32("tank.metrics_too_old")

	// metric add_to_saving_chunk is points received - by the primary node - for the most recent chunk
	// when that chunk is already being saved (or has been saved).
	// this indicates that your GC is actively sealing chunks and saving them before you have the chance to send
	// your (infrequent) updates.  The primary won't add them to its in-memory chunks, but secondaries will
	// (because they are never in "saving" state for them), see below.
	addToSavingChunk = stats.NewCounter32("tank.add_to_saving_chunk")

	// metric add_to_saved_chunk is points received - by a secondary node - for the most recent chunk when that chunk
	// has already been saved by a primary.  A secondary can add this data to its chunks.
	addToSavedChunk = stats.NewCounter32("tank.add_to_saved_chunk")

	memToIterDuration = stats.NewLatencyHistogram15s32("mem.to_iter")
	persistDuration   = stats.NewLatencyHistogram15s32("tank.persist")

	// metric metrics_active is the amount of currently known metrics (excl rollup series), measured every second
	metricsActive = stats.NewGauge32("tank.metrics_active")
	// metric gc_metric is the amount of times the metrics GC is about to inspect a metric (series)
	gcMetric = stats.NewCounter32("tank.gc_metric")
)
