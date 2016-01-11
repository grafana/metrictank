package main

import (
	"github.com/raintank/raintank-metric/metricdef"
	"sync"
	"time"
)

type meta struct {
	interval   int
	targetType string
}

type MetaCache struct {
	intervals map[string]meta
	lock      sync.RWMutex
}

func NewMetaCache() *MetaCache {
	return &MetaCache{
		make(map[string]meta),
		sync.RWMutex{},
	}
}

func (mc *MetaCache) Add(key string, interval int, targetType string) {
	mc.lock.Lock()
	mc.intervals[key] = meta{interval, targetType}
	mc.lock.Unlock()
}

func (mc *MetaCache) Get(key string) meta {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	return mc.intervals[key]
}

func (mc *MetaCache) UpdateReq(req *Req) error {
	// note: the metacache is clearly not a perfect all-knowning entity, it just knows the last interval of metrics seen since program start
	// and we assume we can use that interval through history.
	// TODO: no support for interval changes, missing datablocks, ...
	meta := mc.Get(req.key)

	if meta.interval == 0 {
		metricMetaCacheMiss.Inc(1)
		pre := time.Now()
		def, err := metricdef.GetMetricDefinition(req.key)
		if err != nil {
			return err
		}
		metricMetaGetDuration.Value(time.Now().Sub(pre))
		req.rawInterval = uint32(def.Interval)
	} else {
		req.rawInterval = uint32(meta.interval)
		metricMetaCacheHit.Inc(1)
	}
	return nil
}
