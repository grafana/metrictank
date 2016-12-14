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

type CacheChunk struct {
	Ts    uint32
	Next  uint32
	Prev  uint32
	Itgen iter.IterGen
	lru   *LRU
}

// this assumes we have a lock
func (cc *CacheChunk) setNext(next uint32) {
	cc.Next = next
}

type CCache struct {
	sync.RWMutex
	lru         *LRU
	metricCache map[string]*CCacheMetric
}

func NewChunkCache() *CCache {
	return &CCache{
		lru:         NewLRU(),
		metricCache: make(map[string]*CCacheMetric),
	}
}

func (c *CCache) Add(metric string, prev uint32, itergen iter.IterGen) error {
	c.Lock()
	if _, ok := c.metricCache[metric]; !ok {
		ts := itergen.Ts()

		// adding the new metric to the lru
		c.lru.touch(metric)

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
		c.lru.touch(metric)
		return cm.Search(from, until)
	} else {
		return nil
	}
}
