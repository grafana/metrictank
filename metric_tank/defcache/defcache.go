// package defcache wraps our index with performance metrics, a mutex for threadsafe access
// and synchronisation with a definition store
package defcache

import (
	"math/rand"
	"sync"
	"time"

	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/metric_tank/idx"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/schema"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	metricsToEsOK           met.Count
	metricsToEsFail         met.Count
	esPutDuration           met.Timer // note that due to our use of bulk indexer, most values will be very fast with the occasional "outlier" which triggers a flush
	idxPruneDuration        met.Timer
	idxGetDuration          met.Timer
	idxListDuration         met.Timer
	idxMatchLiteralDuration met.Timer
	idxMatchPrefixDuration  met.Timer
	idxMatchTrigramDuration met.Timer
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
	idx.Idx
	defsStore metricdef.Defs
}

func New(defsStore metricdef.Defs, stats met.Backend) *DefCache {
	metricsToEsOK = stats.NewCount("metrics_to_es.ok")
	metricsToEsFail = stats.NewCount("metrics_to_es.fail")
	esPutDuration = stats.NewTimer("es_put_duration", 0)
	idxPruneDuration = stats.NewTimer("idx.prune_duration", 0)
	idxGetDuration = stats.NewTimer("idx.get_duration", 0)
	idxListDuration = stats.NewTimer("idx.list_duration", 0)
	idxMatchLiteralDuration = stats.NewTimer("idx.match_literal_duration", 0)
	idxMatchPrefixDuration = stats.NewTimer("idx.match_prefix_duration", 0)
	idxMatchTrigramDuration = stats.NewTimer("idx.match_trigram_duration", 0)

	d := &DefCache{
		sync.RWMutex{},
		*idx.New(),
		defsStore,
	}
	go d.Prune()
	d.Backfill()
	d.defsStore.SetAsyncResultCallback(d.AsyncResultCallback)
	return d
}

func (dc *DefCache) Prune() {
	t := time.Tick(3 * time.Minute)
	for range t {
		// there's some fragments that occur in a whole lot of metrics
		// for example 'litmus'
		// this only retains the trigram postlists in the index if <20%
		// of the metrics contain them.  this keeps memory usage down
		// and makes queries faster
		pre := time.Now()
		dc.Lock()
		dc.Idx.Prune(0.20)
		dc.Unlock()
		idxPruneDuration.Value(time.Now().Sub(pre))
	}
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
			// def.Id must be unique. if we process the same Id twice, bad things will happen.
			for _, def := range met {
				dc.Idx.Add(*def) // gets id auto assigned from 0 and onwards
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

// Adds the metric to the defcache.
// after this function returns, it is safe to modify the data pointed to
func (dc *DefCache) Add(metric *schema.MetricData) {
	dc.Lock()
	mdef := dc.GetById(idx.MetricID(metric.Id))
	if mdef != nil {
		//If the time diff between this datapoint and the lastUpdate
		// time of the metricDef is greater than 6hours, update the metricDef.
		if mdef.LastUpdate < metric.Time-21600 {
			mdef = schema.MetricDefinitionFromMetricData(metric)
			dc.Update(*mdef)
			dc.Unlock()
			dc.addToES(mdef)
		} else {
			dc.Unlock()
		}
	} else {
		dc.Unlock()
		mdef = schema.MetricDefinitionFromMetricData(metric)
		// now that we have the mdef, let's make sure we only add this once concurrently.
		// because addToES is pretty expensive and we should only call AddRef once.
		dc.Lock()
		def := dc.GetById(idx.MetricID(metric.Id))
		if def != nil {
			// someone beat us to it. nothing left to do
			dc.Unlock()
			return
		}
		dc.Idx.Add(*mdef)
		dc.Unlock()
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
	}
	esPutDuration.Value(time.Now().Sub(pre))
}

// make defcache aware of asynchronous index calls of metric definitions to a defstore (ES) succeeding for a particular def or not.
// if ok, nothing to do
// if not ok, we pretend it was updated 5~5.5 hours ago, so that we'll retry in half an hour to an hour if/when a new one comes in
// so yes, there is a small chance we won't get to that if no new data comes in, which is something to address later.
func (dc *DefCache) AsyncResultCallback(id string, ok bool) {
	if ok {
		return
	}
	dc.Lock()
	mdef := dc.GetById(idx.MetricID(id))
	if mdef == nil {
		dc.Unlock()
		log.Error(3, "got async callback with ES result %t for %q, however it does not exist in the internal index", ok, id)
		return
	}
	// normally we have to do dc.Update(*mdef) but in this case we can just modify the data pointed to, e.g. in the index.
	// we pretend the mdef was last updated some random time between 5h ago and 5h30min ago
	mdef.LastUpdate = time.Now().Unix() - 5*3600 - int64(rand.Intn(30*60))
	dc.Unlock()
}

// Get gets a metricdef by metric id
// note: the defcache is clearly not a perfect all-knowning entity, it just knows the last interval of metrics seen since program start
// and we assume we can use that interval through history.
// TODO: no support for interval changes, ...
// note: do *not* modify the pointed-to data, as it will affect the data in the index!
func (dc *DefCache) Get(id string) *schema.MetricDefinition {
	pre := time.Now()
	dc.RLock()
	def := dc.GetById(idx.MetricID(id))
	dc.RUnlock()
	idxGetDuration.Value(time.Now().Sub(pre))
	return def
}

// Find returns the results of a pattern match.
// note: do *not* modify the pointed-to data, as it will affect the data in the index!
func (dc *DefCache) Find(org int, query string) ([]idx.Glob, []*schema.MetricDefinition) {
	pre := time.Now()
	dc.RLock()
	mt, globs, defs := dc.Match(org, query)
	dc.RUnlock()
	switch mt {
	case idx.MatchLiteral:
		idxMatchLiteralDuration.Value(time.Now().Sub(pre))
	case idx.MatchPrefix:
		idxMatchPrefixDuration.Value(time.Now().Sub(pre))
	case idx.MatchTrigram:
		idxMatchTrigramDuration.Value(time.Now().Sub(pre))
	}
	return globs, defs
}

// List provides all metricdefs based on the provided org. if it is:
// -1, then all metricdefs for all orgs are returned
// any other org, then all defs for that org, as well as -1 are returned
// note: do *not* modify the pointed-to data, as it will affect the data in the index!
func (dc *DefCache) List(org int) []*schema.MetricDefinition {
	pre := time.Now()
	dc.RLock()
	defs := dc.Idx.List(org)
	dc.RUnlock()
	idxListDuration.Value(time.Now().Sub(pre))
	return defs
}
