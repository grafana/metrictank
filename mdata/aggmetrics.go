package mdata

import (
	"sync"
	"time"

	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/worldping-api/pkg/log"
)

type AggMetrics struct {
	store Store
	sync.RWMutex
	Metrics        map[string]*AggMetric
	chunkSpan      uint32
	numChunks      uint32
	aggSettings    []AggSetting // for now we apply the same settings to all AggMetrics. later we may want to have different settings.
	chunkMaxStale  uint32
	metricMaxStale uint32
	ttl            uint32
	gcInterval     time.Duration
}

func NewAggMetrics(store Store, chunkSpan, numChunks, chunkMaxStale, metricMaxStale uint32, ttl uint32, gcInterval time.Duration, aggSettings []AggSetting) *AggMetrics {
	ms := AggMetrics{
		store:          store,
		Metrics:        make(map[string]*AggMetric),
		chunkSpan:      chunkSpan,
		numChunks:      numChunks,
		aggSettings:    aggSettings,
		chunkMaxStale:  chunkMaxStale,
		metricMaxStale: metricMaxStale,
		ttl:            ttl,
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
		if !cluster.ThisNode.IsPrimary() {
			continue
		}
		log.Info("checking for stale chunks that need persisting.")
		now := uint32(time.Now().Unix())
		chunkMinTs := now - (now % ms.chunkSpan) - uint32(ms.chunkMaxStale)
		metricMinTs := now - (now % ms.chunkSpan) - uint32(ms.metricMaxStale)

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
			if stale := a.GC(chunkMinTs, metricMinTs); stale {
				log.Info("metric %s is stale. Purging data from memory.", key)
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

func (ms *AggMetrics) GetOrCreate(key string) Metric {
	ms.Lock()
	m, ok := ms.Metrics[key]
	if !ok {
		m = NewAggMetric(ms.store, key, ms.chunkSpan, ms.numChunks, ms.ttl, ms.aggSettings...)
		ms.Metrics[key] = m
		metricsActive.Set(len(ms.Metrics))
	}
	ms.Unlock()
	return m
}

func (ms *AggMetrics) AggSettings() []AggSetting {
	return ms.aggSettings
}
