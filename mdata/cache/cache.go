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
	// (1024 ^ 3) * 4 = 4294967296 = 4G
	flags.Uint64Var(&maxSize, "max-size", 4294967296, "Maximum size of chunk cache in bytes")
	globalconf.Register("chunk-cache", flags)
}

type CCache struct {
	sync.RWMutex
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
	var res bool

	c.Lock()
	defer c.Unlock()

	if ccm, ok := c.metricCache[metric]; !ok {
		var ccm *CCacheMetric
		ccm = NewCCacheMetric()
		res = ccm.Init(prev, itergen)
		if !res {
			return false
		}
		c.metricCache[metric] = ccm
	} else {
		res = ccm.Add(prev, itergen)
		if !res {
			return false
		}
	}
	c.accnt.Add(metric, itergen.Ts(), itergen.Size())

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
	var res *CCSearchResult
	var hit iter.IterGen

	c.RLock()
	defer c.RUnlock()

	if cm, ok := c.metricCache[metric]; ok {
		res = cm.Search(from, until)
		for _, hit = range res.Start {
			c.accnt.Hit(metric, hit.Ts())
		}
		for _, hit = range res.End {
			c.accnt.Hit(metric, hit.Ts())
		}
		return res
	} else {
		return nil
	}
}

func (c *CCache) evictLoop() {
	evictQ := c.accnt.GetEvictQ()

	for target := range evictQ {
		// keeping these locks as short as possible to not slow down request handling
		// many short ones should impact the response times less than a few long ones
		c.Lock()
		if met, ok := c.metricCache[target.Metric]; ok {
			delete(met.chunks, target.Ts)
			if len(met.chunks) == 0 {
				delete(c.metricCache, target.Metric)
			}
		}
		c.Unlock()
	}
}
