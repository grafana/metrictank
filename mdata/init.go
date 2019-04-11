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

const (
	discardReasonLabel = "reason"

	sampleOutOfOrder = "sample-out-of-order"
	receivedTooLate  = "received-too-late"
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

	// metric tank.metrics_too_old is points that go back in time beyond the scope of the optional reorder window.
	// these points will end up being dropped and lost.
	metricsTooOld = stats.NewCounterRate32("tank.metrics_too_old")

	// metric tank.add_to_closed_chunk is points received for the most recent chunk
	// when that chunk is already being "closed", ie the end-of-stream marker has been written to the chunk.
	// this indicates that your GC is actively sealing chunks and saving them before you have the chance to send
	// your (infrequent) updates.  Any points revcieved for a chunk that has already been closed are discarded.
	addToClosedChunk = stats.NewCounterRate32("tank.add_to_closed_chunk")

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

	schemasFile = "/etc/metrictank/storage-schemas.conf"
	aggFile     = "/etc/metrictank/storage-aggregation.conf"

	promActiveMetrics = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "metrictank",
		Name:      "metrics_active",
		Help:      "Current # of active metrics",
	}, []string{"org"})

	PromDiscardedSamples = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "metrictank",
		Name:      "discarded_samples_total",
		Help:      "Total # of samples that were discarded",
	}, []string{discardReasonLabel, "org"})
)

func ConfigSetup() {
	retentionConf := flag.NewFlagSet("retention", flag.ExitOnError)
	retentionConf.StringVar(&schemasFile, "schemas-file", "/etc/metrictank/storage-schemas.conf", "path to storage-schemas.conf file")
	retentionConf.StringVar(&aggFile, "aggregations-file", "/etc/metrictank/storage-aggregation.conf", "path to storage-aggregation.conf file")
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
