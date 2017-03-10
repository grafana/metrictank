// Package mdata stands for "managed data" or "metrics data" if you will
// it has all the stuff to keep metric data in memory, store it, and synchronize
// save states over the network
package mdata

import (
	"io/ioutil"
	"log"
	"time"

	"github.com/raintank/metrictank/conf"
	"github.com/raintank/metrictank/mdata/matchcache"
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
	Schemas      conf.Schemas
	Aggregations conf.Aggregations
	schemasCache *matchcache.Cache
	aggsCache    *matchcache.Cache

	schemasFile = "/etc/metrictank/storage-schemas.conf"
	aggFile     = "/etc/metrictank/storage-aggregation.conf"
)

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
		Aggregations = conf.NewAggregations()
	}
	Cache(time.Hour, time.Hour)
}

func Cache(cleanInterval, expireAfter time.Duration) {
	schemasCache = matchcache.New(cleanInterval, expireAfter)
	aggsCache = matchcache.New(cleanInterval, expireAfter)
}
