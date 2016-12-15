package cache

import (
	"flag"
	"sync"

	"github.com/raintank/metrictank/iter"
	accnt "github.com/raintank/metrictank/mdata/cache/accnt"
	"github.com/rakyll/globalconf"
)

var (
	maxSize uint64
)

func ConfigSetup() {
	flags := flag.NewFlagSet("chunk-cache", flag.ExitOnError)
	// (1024 ^ 3) * 4 = 4294967296
	flags.Uint64Var(&maxSize, "max-size", 4294967296, "Maximum size of chunk cache in bytes")
	globalconf.Register("chunk-cache", flags)
}

type CCache struct {
	sync.RWMutex
	evictMutex  sync.Mutex
	metricCache map[string]*CCacheMetric
	accnt       accnt.Accnt
}

func NewCCache() *CCache {
	cc := &CCache{
		metricCache: make(map[string]*CCacheMetric),
		accnt:       accnt.NewAccnt(maxSize),
	}
	go cc.evictLoop()
	return cc
}

func (c *CCache) Add(metric string, prev uint32, itergen iter.IterGen) bool {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.metricCache[metric]; !ok {
		ccm := NewCCacheMetric(metric, c.accnt)
		res := ccm.Init(prev, itergen)
		if !res {
			return false
		}
		c.metricCache[metric] = ccm
	} else {
		c.metricCache[metric].Add(prev, itergen)
	}

	return true
}

type CCSearchResult struct {
	From     uint32
	Until    uint32
	Complete bool
	Start    []iter.IterGen
	End      []iter.IterGen
}

func (c *CCache) Search(metric string, from uint32, until uint32) *CCSearchResult {
	c.RLock()
	defer c.RUnlock()

	if cm, ok := c.metricCache[metric]; ok {
		return cm.Search(from, until)
	} else {
		return nil
	}
}

func (c *CCache) evictLoop() {
	evictQ := c.accnt.GetEvictQ()

	for target := range evictQ {
		c.Lock()
		if met, ok := c.metricCache[target.Metric]; ok {
			// in both of these cases we just drop the whole metric
			delete(met.chunks, target.Ts)
			if len(met.chunks) == 0 {
				delete(c.metricCache, target.Metric)
			}
		}
		c.Unlock()
	}
}
