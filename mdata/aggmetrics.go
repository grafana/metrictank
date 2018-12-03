package mdata

import (
	"strconv"
	"sync"
	"time"

	"github.com/grafana/metrictank/mdata/cache"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
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
	Metrics        map[uint32]map[schema.Key]*AggMetric
	chunkMaxStale  uint32
	metricMaxStale uint32
	gcInterval     time.Duration
}

func NewAggMetrics(store Store, cachePusher cache.CachePusher, dropFirstChunk bool, chunkMaxStale, metricMaxStale uint32, gcInterval time.Duration) *AggMetrics {
	ms := AggMetrics{
		store:          store,
		cachePusher:    cachePusher,
		dropFirstChunk: dropFirstChunk,
		Metrics:        make(map[uint32]map[schema.Key]*AggMetric),
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
		// we only need to lock long enough to get the list of orgs, then for each org
		// get the list of active metrics.
		// It doesn't matter if new orgs or metrics are added while we iterate these lists.
		ms.RLock()
		orgs := make([]uint32, 0, len(ms.Metrics))
		for o := range ms.Metrics {
			orgs = append(orgs, o)
		}
		ms.RUnlock()
		for _, org := range orgs {
			orgActiveMetrics := promActiveMetrics.WithLabelValues(strconv.Itoa(int(org)))
			keys := make([]schema.Key, 0, len(ms.Metrics[org]))
			ms.RLock()
			for k := range ms.Metrics[org] {
				keys = append(keys, k)
			}
			ms.RUnlock()
			for _, key := range keys {
				gcMetric.Inc()
				ms.RLock()
				a := ms.Metrics[org][key]
				ms.RUnlock()
				if a.GC(now, chunkMinTs, metricMinTs) {
					log.Debugf("metric %s is stale. Purging data from memory.", key)
					ms.Lock()
					delete(ms.Metrics[org], key)
					orgActiveMetrics.Set(float64(len(ms.Metrics[org])))
					ms.Unlock()
				}
			}
			ms.RLock()
			orgActive := len(ms.Metrics[org])
			orgActiveMetrics.Set(float64(orgActive))
			ms.RUnlock()

			// If this org has no keys, then delete the org from the map
			if orgActive == 0 {
				// To prevent races, we need to check that there are still no metrics for the org while holding a write lock
				ms.Lock()
				orgActive = len(ms.Metrics[org])
				if orgActive == 0 {
					delete(ms.Metrics, org)
				}
				ms.Unlock()
			}
		}

		// Get the totalActive across all orgs.
		totalActive := 0
		ms.RLock()
		for o := range ms.Metrics {
			totalActive += len(ms.Metrics[o])
		}
		ms.RUnlock()
		metricsActive.Set(totalActive)
	}
}

func (ms *AggMetrics) Get(key schema.MKey) (Metric, bool) {
	var m *AggMetric
	ms.RLock()
	_, ok := ms.Metrics[key.Org]
	if ok {
		m, ok = ms.Metrics[key.Org][key.Key]
	}
	ms.RUnlock()
	return m, ok
}

func (ms *AggMetrics) GetOrCreate(key schema.MKey, schemaId, aggId uint16) Metric {
	var m *AggMetric
	// in the most common case, it's already there and an Rlock is all we need
	ms.RLock()
	_, ok := ms.Metrics[key.Org]
	if ok {
		m, ok = ms.Metrics[key.Org][key.Key]
	}
	ms.RUnlock()
	if ok {
		return m
	}

	k := schema.AMKey{
		MKey: key,
	}

	agg := Aggregations.Get(aggId)
	confSchema := Schemas.Get(schemaId)

	// if it wasn't there, get the write lock and prepare to add it
	// but first we need to check again if someone has added it in
	// the meantime (quite rare, but anyway)
	ms.Lock()
	if _, ok := ms.Metrics[key.Org]; !ok {
		ms.Metrics[key.Org] = make(map[schema.Key]*AggMetric)
	}
	m, ok = ms.Metrics[key.Org][key.Key]
	if ok {
		ms.Unlock()
		return m
	}
	m = NewAggMetric(ms.store, ms.cachePusher, k, confSchema.Retentions, confSchema.ReorderWindow, &agg, ms.dropFirstChunk)
	ms.Metrics[key.Org][key.Key] = m
	active := len(ms.Metrics[key.Org])
	ms.Unlock()
	metricsActive.Inc()
	promActiveMetrics.WithLabelValues(strconv.Itoa(int(key.Org))).Set(float64(active))
	return m
}
