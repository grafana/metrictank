// Package mdata stands for "managed data" or "metrics data" if you will
// it has all the stuff to keep metric data in memory, store it, and synchronize
// save states over the network
package mdata

import (
	"log"

	"github.com/lomik/go-carbon/persister"
	"github.com/raintank/metrictank/stats"
)

var (
	LogLevel int

	// metric tank.chunk_operations.create is a counter of how many chunks are created
	chunkCreate = stats.NewCounter32("tank.chunk_operations.create")

	// metric tank.chunk_operations.clear is a counter of how many chunks are cleared (replaced by new chunks)
	chunkClear = stats.NewCounter32("tank.chunk_operations.clear")

	// metric tank.metrics_too_old is points that go back in time.
	// E.g. for any given series, when a point has a timestamp
	// that is not higher than the timestamp of the last written timestamp for that series.
	metricsTooOld = stats.NewCounter32("tank.metrics_too_old")

	// metric tank.add_to_closed_chunk is points received for the most recent chunk
	// when that chunk is already being "closed", ie the end-of-stream marker has been written to the chunk.
	// this indicates that your GC is actively sealing chunks and saving them before you have the chance to send
	// your (infrequent) updates.  Any points revcieved for a chunk that has already been closed are discarded.
	addToClosedChunk = stats.NewCounter32("tank.add_to_closed_chunk")

	// metric mem.to_iter is how long it takes to transform in-memory chunks to iterators
	memToIterDuration = stats.NewLatencyHistogram15s32("mem.to_iter")

	// metric tank.persist is how long it takes to persist a chunk (and chunks preceding it)
	// this is subject to backpressure from the store when the store's queue runs full
	persistDuration = stats.NewLatencyHistogram15s32("tank.persist")

	// metric tank.metrics_active is the number of currently known metrics (excl rollup series), measured every second
	metricsActive = stats.NewGauge32("tank.metrics_active")

	// metric tank.gc_metric is the number of times the metrics GC is about to inspect a metric (series)
	gcMetric = stats.NewCounter32("tank.gc_metric")

	// set either via ConfigProcess or from the unit tests. other code should not touch
	Schemas      persister.WhisperSchemas
	Aggregations *persister.WhisperAggregation

	schemasFile = "/etc/raintank/storage-schemas.conf"
	aggFile     = "/etc/raintank/storage-aggregation.conf"
)

func ConfigProcess() {
	var err error
	Schemas, err = persister.ReadWhisperSchemas(schemasFile)
	if err != nil {
		log.Fatalf("can't read schemas file %q: %s", schemasFile, err.Error())
	}
	var defaultFound bool
	for _, schema := range Schemas {
		if schema.Pattern.String() == ".*" {
			defaultFound = true
		}
		if len(schema.Retentions) == 0 {
			log.Fatal(4, "retention setting cannot be empty")
		}
	}
	if !defaultFound {
		// good graphite health (not sure what graphite does if there's no .*)
		// but we definitely need to always be able to determine which interval to use
		log.Fatal(4, "storage-conf does not have a default '.*' pattern")
	}
	Aggregations, err = persister.ReadWhisperAggregation(aggFile)
	if err != nil {
		log.Fatalf("can't read storage-aggregation file %q: %s", aggFile, err.Error())
	}
}
