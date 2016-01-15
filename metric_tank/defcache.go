package main

import (
	"errors"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/schema"
	"sync"
	"time"
)

// design notes:
// MT pulls in all definitions when it starts up.
// those "old" ones + whatever it sees as inputs from the metrics queue
// is enough for it to always know the complete current state
// nothing should update ES "behind its back", so we never need to pull
// from ES other then at startup.
// but other MT instances may update ES while we are down, so ES is a good
// place to pull from, until the performance is demonstrably too slow.
// there are some vectors here for race conditions but we can work those out
// later, perhaps when tacking the multiple-intervals work

type DefCache struct {
	sync.RWMutex
	defs map[string]*schema.MetricDefinition
}

func NewDefCache() *DefCache {
	return &DefCache{
		defs: make(map[string]*schema.MetricDefinition),
	}
	// TODO: initialize state from ES, something like searchDSL.Scroll(
}

func (dc *DefCache) Add(metric *schema.MetricData) {
	id := metric.GetId()
	dc.Lock()
	mdef, ok := dc.defs[id]
	dc.Unlock()
	if ok {
		if mdef.LastUpdate < time.Now().Unix()-600 {
			dc.addToES(mdef)
		}
	} else {
		mdef = schema.MetricDefinitionFromMetricData(metric)
		dc.Lock()
		add := false
		_, ok := dc.defs[id]
		if !ok {
			dc.defs[id] = mdef
			add = true
		}
		dc.Unlock()
		if add {
			dc.addToES(mdef)
		}
	}
}

func (dc *DefCache) addToES(mdef *schema.MetricDefinition) {
	pre := time.Now()
	err := metricdef.IndexMetric(mdef)
	if err != nil {
		log.Error(3, "couldn't index to ES %s: %s", mdef.Id, err)
		//metricsToEsFail.Inc(int64(len(ms.Metrics) - i))
		//metricsToEsOK.Inc(int64(len(ms.Metrics)))
		esPutDuration.Value(time.Now().Sub(pre))
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
		return errors.New("not found")
	} else {
		req.rawInterval = uint32(def.Interval)
		metricDefCacheHit.Inc(1)
	}
	return nil
}

// now deprecated:
// metricMetaGetDuration.Value(time.Now().Sub(pre))
