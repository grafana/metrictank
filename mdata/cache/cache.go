package cache

import (
	"flag"
	"sync"

	"github.com/raintank/metrictank/iter"
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
	lru         *LRU
	metricCache map[string]*CCacheMetric
	accounting  Accounting
}

func NewChunkCache() *CCache {
	return &CCache{
		lru:         NewLRU(),
		metricCache: make(map[string]*CCacheMetric),
		accounting:  *NewAccounting(),
	}
}

func (c *CCache) Add(metric string, prev uint32, itergen iter.IterGen) error {
	// add metric to lru and update accounting
	go c.lru.touch(metric)

	c.Lock()
	if _, ok := c.metricCache[metric]; !ok {
		ts := itergen.Ts()

		c.accounting.Add(metric, ts, itergen.Size())
		if c.accounting.GetTotal() >= maxSize {
			// evict the least recent used 20% of the current cache content
			go c.evict(20)
		}

		// initializing a new linked list with head and tail
		c.metricCache[metric] = &CCacheMetric{
			oldest: ts,
			newest: ts,
			chunks: map[uint32]*CacheChunk{
				ts: &CacheChunk{
					ts,
					0,
					0,
					itergen,
					NewLRU(),
				},
			},
			lru: NewLRU(),
		}
	} else {
		c.metricCache[metric].Add(prev, itergen)
	}
	c.Unlock()

	return nil
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
		// updating the metrics lru
		go c.lru.touch(metric)

		return cm.Search(from, until)
	} else {
		return nil
	}
}

func (c *CCache) evict(percent uint32) {
}
