package mdata

import (
	"sync"
	"time"

	"github.com/grafana/metrictank/mdata/cache"
	"github.com/raintank/worldping-api/pkg/log"
)

type AggMetrics struct {
	store          Store
	cachePusher    cache.CachePusher
	dropFirstChunk bool
	sync.RWMutex
	Metrics        map[string]*AggMetric
	chunkMaxStale  uint32
	metricMaxStale uint32
	gcInterval     time.Duration
}

func NewAggMetrics(store Store, cachePusher cache.CachePusher, dropFirstChunk bool, chunkMaxStale, metricMaxStale uint32, gcInterval time.Duration) *AggMetrics {
	ms := AggMetrics{
		store:          store,
		cachePusher:    cachePusher,
		dropFirstChunk: dropFirstChunk,
		Metrics:        make(map[string]*AggMetric),
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
		keys := make([]string, 0, len(ms.Metrics))
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
				ms.Unlock()
			}
		}

	}
}

func (ms *AggMetrics) Get(key string) (Metric, bool) {
	ms.RLock()
	m, ok := ms.Metrics[key]
	ms.RUnlock()
	return m, ok
}

func (ms *AggMetrics) GetOrCreate(key, name string, schemaId, aggId uint16) Metric {
	ms.Lock()
	m, ok := ms.Metrics[key]
	if !ok {
		agg := Aggregations.Get(aggId)
		schema := Schemas.Get(schemaId)
		m = NewAggMetric(ms.store, ms.cachePusher, key, schema.Retentions, schema.ReorderWindow, &agg, ms.dropFirstChunk)
		ms.Metrics[key] = m
		metricsActive.Set(len(ms.Metrics))
	}
	ms.Unlock()
	return m
}
