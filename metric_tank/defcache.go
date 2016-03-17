package main

import (
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
	defsById  map[string]*schema.MetricDefinition // by hashed id
	defsByKey map[string]*schema.MetricDefinition // by graphite key or "Name" in the def
	defsStore metricdef.Defs
}

func NewDefCache(defsStore metricdef.Defs) *DefCache {
	d := &DefCache{
		defsById:  make(map[string]*schema.MetricDefinition),
		defsByKey: make(map[string]*schema.MetricDefinition),
		defsStore: defsStore,
	}
	d.Backfill()
	return d
}

// backfill definitions from ES
// in theory, there is a race between defs from ES and from nsq
// in practice, it doesn't matter: you're only supposed to query MT
// after a while, after which the defs surely have stabilized.
func (dc *DefCache) Backfill() {
	total := 0
	add := func(met []*schema.MetricDefinition) {
		if len(met) > 0 {
			total += len(met)
			dc.Lock()
			for _, def := range met {
				dc.defsById[def.Id] = def
				dc.defsByKey[def.Name] = def
			}
			dc.Unlock()
		}
	}
	met, scroll_id, err := dc.defsStore.GetMetrics("")
	if err != nil {
		log.Error(3, "Could not backfill from ES: %s", err)
		return
	}
	add(met)
	for scroll_id != "" {
		met, scroll_id, err = dc.defsStore.GetMetrics(scroll_id)
		if err != nil {
			log.Error(3, "Could not backfill from ES: %s", err)
			return
		}
		add(met)
	}
	log.Debug("backfilled %d metric definitions", total)
}

func (dc *DefCache) Add(metric *schema.MetricData) {
	dc.Lock()
	mdef, ok := dc.defsById[metric.Id]
	dc.Unlock()
	if ok {
		//If the time diff between this datapoint and the lastUpdate
		// time of the metricDef is greater than 6hours, update the metricDef.
		if mdef.LastUpdate < metric.Time-21600 {
			mdef = schema.MetricDefinitionFromMetricData(metric)
			dc.addToES(mdef)
		}
	} else {
		mdef = schema.MetricDefinitionFromMetricData(metric)
		dc.addToES(mdef)
	}
}

func (dc *DefCache) addToES(mdef *schema.MetricDefinition) {
	pre := time.Now()
	err := dc.defsStore.IndexMetric(mdef)
	// NOTE: indexing to ES is done asyncrounously using the bulkAPI.
	// so an error here is just an error adding the document to the
	// bulkAPI buffer.
	if err != nil {
		log.Error(3, "couldn't index to ES %s: %s", mdef.Id, err)
		metricsToEsFail.Inc(1)
	} else {
		metricsToEsOK.Inc(1)
		dc.Lock()
		dc.defsById[mdef.Id] = mdef
		dc.defsByKey[mdef.Name] = mdef
		dc.Unlock()
	}
	esPutDuration.Value(time.Now().Sub(pre))
}

func (dc *DefCache) Get(id string) (*schema.MetricDefinition, bool) {
	dc.RLock()
	def, ok := dc.defsById[id]
	dc.RUnlock()
	return def, ok
}

func (dc *DefCache) GetByKey(key string) (*schema.MetricDefinition, bool) {
	dc.RLock()
	def, ok := dc.defsByKey[key]
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
		return errMetricNotFound
	} else {
		req.rawInterval = uint32(def.Interval)
		metricDefCacheHit.Inc(1)
	}
	return nil
}
