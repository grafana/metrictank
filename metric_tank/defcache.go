package main

import (
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/schema"
	"sync"
	"time"
)

type DefCache struct {
	sync.RWMutex
	defs map[string]*schema.MetricDefinition
}

func NewDefCache() *DefCache {
	return &DefCache{
		defs: make(map[string]*schema.MetricDefinition),
	}
	// TODO: initialize state from local disk or cassandra
	// TODO: periodically sync state to local disk
}

func (dc *DefCache) Add(metric *schema.MetricData) {
	id := metric.GetId()
	dc.Lock()
	_, ok := dc.defs[id]
	dc.Unlock()
	if !ok {
		mdef := schema.MetricDefinitionFromMetricData(metric)
		dc.Lock()
		dc.defs[id] = mdef
		dc.Unlock()
	}

}

func (dc *DefCache) Get(key string) (*schema.MetricDefinition, bool) {
	dc.RLock()
	def, ok := dc.defs[key]
	dc.RUnlock()
	return def, ok
}

func (dc *DefCache) UpdateReq(req *Req) error {
	// note: the defcache is clearly not a perfect all-knowning entity, it just knows the last interval of metrics seen since program start
	// and we assume we can use that interval through history.
	// TODO: no support for interval changes, missing datablocks, ...
	def, ok := dc.Get(req.key)

	if !ok {
		metricDefCacheMiss.Inc(1)
		pre := time.Now()
		def, err := metricdef.GetMetricDefinition(req.key)
		if err != nil {
			return err
		}
		metricMetaGetDuration.Value(time.Now().Sub(pre))
		req.rawInterval = uint32(def.Interval)
	} else {
		req.rawInterval = uint32(def.Interval)
		metricDefCacheHit.Inc(1)
	}
	return nil
}

// TODO: re-introduce the following things:
//		log.Error(3, "couldn't index to ES %s: %s", m.GetId(), err)
//		metricsToEsFail.Inc(int64(len(ms.Metrics) - i))
//metricsToEsOK.Inc(int64(len(ms.Metrics)))
//esPutDuration.Value(time.Now().Sub(pre))
