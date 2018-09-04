package mdata

import (
	"sync"
	"time"

	"github.com/grafana/metrictank/mdata/cache"
	"github.com/raintank/schema"
	"github.com/raintank/worldping-api/pkg/log"
)

// AggMetrics is an in-memory store of AggMetric objects
// note: they are keyed by MKey here because each
// AggMetric manages access to, and references of,
// their rollup archives themselves
type AggMetrics struct {
	store          Store
	cachePusher    cache.CachePusher
	dropFirstChunk bool
	sync.RWMutex
	Metrics        map[schema.MKey]*AggMetric
	chunkMaxStale  uint32
	metricMaxStale uint32
	gcInterval     time.Duration
}

func NewAggMetrics(store Store, cachePusher cache.CachePusher, dropFirstChunk bool, chunkMaxStale, metricMaxStale uint32, gcInterval time.Duration) *AggMetrics {
	ms := AggMetrics{
		store:          store,
		cachePusher:    cachePusher,
		dropFirstChunk: dropFirstChunk,
		Metrics:        make(map[schema.MKey]*AggMetric),
		chunkMaxStale:  chunkMaxStale,
		metricMaxStale: metricMaxStale,
		gcInterval:     gcInterval,
	}

	// gcInterval = 0 can be useful in tests
	if gcInterval > 0 {
		go ms.GC()
	}
	return &ms
}

// periodically scan chunks and close any that have not received data in a while
func (ms *AggMetrics) GC() {
	for {
		unix := time.Duration(time.Now().UnixNano())
		diff := ms.gcInterval - (unix % ms.gcInterval)
		time.Sleep(diff + time.Minute)
		log.Info("checking for stale chunks that need persisting.")
		now := uint32(time.Now().Unix())
		chunkMinTs := now - uint32(ms.chunkMaxStale)
		metricMinTs := now - uint32(ms.metricMaxStale)

		// as this is the only goroutine that can delete from ms.Metrics
		// we only need to lock long enough to get the list of actives metrics.
		// it doesn't matter if new metrics are added while we iterate this list.
		ms.RLock()
		keys := make([]schema.MKey, 0, len(ms.Metrics))
		for k := range ms.Metrics {
			keys = append(keys, k)
		}
		ms.RUnlock()
		for _, key := range keys {
			gcMetric.Inc()
			ms.RLock()
			a := ms.Metrics[key]
			ms.RUnlock()
			if a.GC(now, chunkMinTs, metricMinTs) {
				log.Debug("metric %s is stale. Purging data from memory.", key)
				ms.Lock()
				delete(ms.Metrics, key)
				metricsActive.Set(len(ms.Metrics))
				promActiveMetrics.Set(float64(len(ms.Metrics)))
				ms.Unlock()
			}
		}

	}
}

func (ms *AggMetrics) Get(key schema.MKey) (Metric, bool) {
	ms.RLock()
	m, ok := ms.Metrics[key]
	ms.RUnlock()
	return m, ok
}

func (ms *AggMetrics) GetOrCreate(key schema.MKey, schemaId, aggId uint16) Metric {

	// in the most common case, it's already there and an Rlock is all we need
	ms.RLock()
	m, ok := ms.Metrics[key]
	ms.RUnlock()
	if ok {
		return m
	}

	k := schema.AMKey{
		MKey: key,
	}

	agg := Aggregations.Get(aggId)
	schema := Schemas.Get(schemaId)

	// if it wasn't there, get the write lock and prepare to add it
	// but first we need to check again if someone has added it in
	// the meantime (quite rare, but anyway)
	ms.Lock()
	m, ok = ms.Metrics[key]
	if ok {
		ms.Unlock()
		return m
	}
	m = NewAggMetric(ms.store, ms.cachePusher, k, schema.Retentions, schema.ReorderWindow, &agg, ms.dropFirstChunk)
	ms.Metrics[key] = m
	active := len(ms.Metrics)
	ms.Unlock()
	metricsActive.Set(active)
	promActiveMetrics.Set(float64(active))
	return m
}
