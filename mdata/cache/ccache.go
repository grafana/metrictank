package cache

import (
	"flag"
	"runtime"
	"sync"

	"github.com/raintank/metrictank/mdata/cache/accnt"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/worldping-api/pkg/log"
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
		accnt:       accnt.NewAccnt(maxSize),
	}
	go cc.evictLoop()
	return cc
}

func (c *CCache) evictLoop() {
	evictQ := c.accnt.GetEvictQ()
	for target := range evictQ {
		c.evict(target)
	}
}

func (c *CCache) Add(metric string, prev uint32, itergen chunk.IterGen) {
	c.Lock()
	defer c.Unlock()

	if ccm, ok := c.metricCache[metric]; !ok {
		ccm = NewCCacheMetric()
		ccm.Init(prev, itergen)
		c.metricCache[metric] = ccm
	} else {
		ccm.Add(prev, itergen)
	}

	c.accnt.AddChunk(metric, itergen.Ts(), itergen.Size())
}

func (c *CCache) evict(target *accnt.EvictTarget) {
	c.Lock()
	defer c.Unlock()
	defer runtime.Gosched()

	if _, ok := c.metricCache[target.Metric]; ok {
		log.Debug("CCache evict: evicting chunk %d on metric %s\n", target.Ts, target.Metric)
		length := c.metricCache[target.Metric].Del(target.Ts)
		if length == 0 {
			delete(c.metricCache, target.Metric)
		}
	}
}

func (c *CCache) Search(metric string, from, until uint32) *CCSearchResult {
	var hit chunk.IterGen
	var cm *CCacheMetric
	var ok bool
	res := &CCSearchResult{
		From:  from,
		Until: until,
	}

	c.RLock()
	defer c.RUnlock()

	if cm, ok = c.metricCache[metric]; !ok {
		// for stats only
		c.accnt.MissMetric()
		return res
	}

	cm.Search(res, from, until)
	if len(res.Start) == 0 && len(res.End) == 0 {
		// for stats only, record a complete miss
		c.accnt.MissMetric()
	} else {

		go func() {
			for _, hit = range res.Start {
				c.accnt.HitChunk(metric, hit.Ts())
			}
			for _, hit = range res.End {
				c.accnt.HitChunk(metric, hit.Ts())
			}
		}()

		// for stats only
		if res.Complete {
			// record a complete hit
			c.accnt.CompleteMetric()
		} else {
			// record a partial hit
			c.accnt.PartialMetric()
		}
	}

	return res
}
