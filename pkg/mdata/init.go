// Package mdata stands for "managed data" or "metrics data" if you will
// it has all the stuff to keep metric data in memory, store it, and synchronize
// save states over the network
package mdata

import (
	"flag"
	"io/ioutil"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/stats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

// Possible reason labels for Prometheus metric discarded_samples_total
const (
	sampleOutOfOrder     = "sample-out-of-order"
	receivedTooLate      = "received-too-late"
	newValueForTimestamp = "new-value-for-timestamp"
	tooFarAhead          = "too-far-in-future"
)

var (
	// metric tank.chunk_operations.create is a counter of how many chunks are created
	chunkCreate = stats.NewCounter32("tank.chunk_operations.create")

	// metric tank.chunk_operations.clear is a counter of how many chunks are cleared (replaced by new chunks)
	chunkClear = stats.NewCounter32("tank.chunk_operations.clear")

	// metric tank.metrics_reordered is the number of points received that are going back in time, but are still
	// within the reorder window. in such a case they will be inserted in the correct order.
	// E.g. if the reorder window is 60 (datapoints) then points may be inserted at random order as long as their
	// ts is not older than the 60th datapoint counting from the newest.
	metricsReordered = stats.NewCounter32("tank.metrics_reordered")

	// metric tank.discarded.sample-out-of-order is points that go back in time beyond the scope of the optional reorder window.
	// these points will end up being dropped and lost.
	discardedSampleOutOfOrder = stats.NewCounterRate32("tank.discarded.sample-out-of-order")

	// metric tank.discarded.sample-too-far-ahead is count of points which got discareded because their timestamp
	// is too far in the future, beyond the limitation of the future tolerance window defined via the
	// retention.future-tolerance-ratio parameter.
	discardedSampleTooFarAhead = stats.NewCounterRate32("tank.discarded.sample-too-far-ahead")

	// metric tank.sample-too-far-ahead is count of points with a timestamp which is too far in the future,
	// beyond the limitation of the future tolerance window defined via the retention.future-tolerance-ratio
	// parameter. it also gets increased if the enforcement of the future tolerance is disabled, this is
	// useful for predicting whether data points would get rejected once enforcement gets turned on.
	sampleTooFarAhead = stats.NewCounterRate32("tank.sample-too-far-ahead")

	// metric tank.discarded.received-too-late is points received for the most recent chunk
	// when that chunk is already being "closed", ie the end-of-stream marker has been written to the chunk.
	// this indicates that your GC is actively sealing chunks and saving them before you have the chance to send
	// your (infrequent) updates.  Any points revcieved for a chunk that has already been closed are discarded.
	discardedReceivedTooLate = stats.NewCounterRate32("tank.discarded.received-too-late")

	// metric tank.discarded.new-value-for-timestamp is points that have timestamps for which we already have data points.
	// these points are discarded.
	// data points can be incorrectly classified as metric tank.discarded.sample-out-of-order even when the timestamp
	// has already been used. This happens in two cases:
	// - when the reorder buffer is enabled, if the point is older than the reorder buffer retention window
	// - when the reorder buffer is disabled, if the point is older than the last data point
	discardedNewValueForTimestamp = stats.NewCounterRate32("tank.discarded.new-value-for-timestamp")

	// metric tank.discarded.unknown is points that have been discarded for unknown reasons.
	discardedUnknown = stats.NewCounterRate32("tank.discarded.unknown")

	// metric tank.total_points is the number of points currently held in the in-memory ringbuffer
	totalPoints = stats.NewGauge64("tank.total_points")

	// metric mem.to_iter is how long it takes to transform in-memory chunks to iterators
	memToIterDuration = stats.NewLatencyHistogram15s32("mem.to_iter")

	// metric tank.persist is how long it takes to persist a chunk (and chunks preceding it)
	// this is subject to backpressure from the store when the store's queue runs full
	persistDuration = stats.NewLatencyHistogram15s32("tank.persist")

	// metric tank.metrics_active is the number of currently known metrics (excl rollup series), measured every second
	metricsActive = stats.NewGauge32("tank.metrics_active")

	// metric tank.gc_metric is the number of times the metrics GC is about to inspect a metric (series)
	gcMetric = stats.NewCounter32("tank.gc_metric")

	// metric recovered_errors.aggmetric.getaggregated.bad-consolidator is how many times we detected an GetAggregated call
	// with an incorrect consolidator specified
	badConsolidator = stats.NewCounter32("recovered_errors.aggmetric.getaggregated.bad-consolidator")

	// metric recovered_errors.aggmetric.getaggregated.bad-aggspan is how many times we detected an GetAggregated call
	// with an incorrect aggspan specified
	badAggSpan = stats.NewCounter32("recovered_errors.aggmetric.getaggregated.bad-aggspan")

	// set either via ConfigProcess or from the unit tests. other code should not touch
	Aggregations conf.Aggregations
	Schemas      conf.Schemas

	schemasFile            = "/etc/metrictank/storage-schemas.conf"
	aggFile                = "/etc/metrictank/storage-aggregation.conf"
	futureToleranceRatio   = uint(10)
	enforceFutureTolerance = true

	promActiveMetrics = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "metrictank",
		Name:      "metrics_active",
		Help:      "Current # of active metrics",
	}, []string{"org"})

	PromDiscardedSamples = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "metrictank",
		Name:      "discarded_samples_total",
		Help:      "Total # of samples that were discarded",
	}, []string{"reason", "org"})
)

func ConfigSetup() {
	retentionConf := flag.NewFlagSet("retention", flag.ExitOnError)
	retentionConf.StringVar(&schemasFile, "schemas-file", "/etc/metrictank/storage-schemas.conf", "path to storage-schemas.conf file")
	retentionConf.StringVar(&aggFile, "aggregations-file", "/etc/metrictank/storage-aggregation.conf", "path to storage-aggregation.conf file")
	retentionConf.UintVar(&futureToleranceRatio, "future-tolerance-ratio", 10, "defines until how far in the future we accept datapoints. defined as a percentage fraction of the raw ttl of the matching retention storage schema")
	retentionConf.BoolVar(&enforceFutureTolerance, "enforce-future-tolerance", true, "enables/disables the enforcement of the future tolerance limitation")
	globalconf.Register("retention", retentionConf, flag.ExitOnError)
}

func ConfigProcess() {
	var err error

	// === read storage-schemas.conf ===

	// graphite behavior: abort on any config reading errors, but skip any rules that have problems.
	// at the end, add a default schema of 7 days of minutely data.
	// we are stricter and don't tolerate any errors, that seems in the user's best interest.

	Schemas, err = conf.ReadSchemas(schemasFile)
	if err != nil {
		log.Fatalf("can't read schemas file %q: %s", schemasFile, err.Error())
	}

	// === read storage-aggregation.conf ===

	// graphite behavior:
	// continue if file can't be read. (e.g. file is optional) but quit if other error reading config
	// always add a default rule with xFilesFactor None and aggregationMethod None
	// (which get interpreted by whisper as 0.5 and avg) at the end.

	// since we can't distinguish errors reading vs parsing, we'll just try a read separately first
	_, err = ioutil.ReadFile(aggFile)
	if err == nil {
		Aggregations, err = conf.ReadAggregations(aggFile)
		if err != nil {
			log.Fatalf("can't read storage-aggregation file %q: %s", aggFile, err.Error())
		}
	} else {
		log.Infof("Could not read %s: %s: using defaults", aggFile, err)
		Aggregations = conf.NewAggregations()
	}
}
