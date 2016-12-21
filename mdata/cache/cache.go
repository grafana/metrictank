package cache

import (
	"flag"
	"sync"

	"github.com/raintank/metrictank/mdata/cache/accnt"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/rakyll/globalconf"
)

var (
	maxSize uint64
)

func init() {
	flags := flag.NewFlagSet("chunk-cache", flag.ExitOnError)
	// (1024 ^ 3) * 4 = 4294967296 = 4G
	flags.Uint64Var(&maxSize, "max-size", 4294967296, "Maximum size of chunk cache in bytes")
	globalconf.Register("chunk-cache", flags)
}

type CCache struct {
	sync.RWMutex

	// one CCacheMetric struct per metric key, indexed by the key
	metricCache map[string]*CCacheMetric

	// accounting for the cache. keeps track of when data needs to be evicted
	// and what should be evicted
	accnt accnt.Accnt
}

type CCSearchResult struct {
	// if this result is Complete == false, then the following cassandra query
	// will need to use this value as from to fill in the missing data
	From uint32

	// just as with the above From, this will need to be used as the new until
	Until uint32

	// if Complete is true then the whole request can be served from cache
	Complete bool

	// if the cache contained the chunk containing the original "from" ts then
	// this slice will hold it as the first element, plus all the subsequent
	// cached chunks. If Complete is true then all chunks are in this slice.
	Start []chunk.IterGen

	// if complete is not true and the original "until" ts is in a cached chunk
	// then this slice will hold it as the first element, plus all the previous
	// ones in reverse order (because the search is seeking in reverse)
	End []chunk.IterGen
}

func NewCCache() *CCache {
	cc := &CCache{
		metricCache: make(map[string]*CCacheMetric),
		accnt:       accnt.NewFlatAccnt(maxSize),
	}
	go cc.evictLoop()
	return cc
}

func (c *CCache) Add(metric string, prev uint32, itergen chunk.IterGen) bool {
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

func (c *CCache) Search(metric string, from uint32, until uint32) *CCSearchResult {
	var res *CCSearchResult
	var hit chunk.IterGen

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
			length := met.Del(target.Ts)
			if length == 0 {
				delete(c.metricCache, target.Metric)
			}
		}
		c.Unlock()
	}
}
