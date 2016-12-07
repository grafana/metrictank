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
	Ts      uint32
	Next    uint32
	Prev    uint32
	itergen iter.IterGen
}

type CCacheMetric struct {
	sync.RWMutex
	chunks map[uint32]CacheChunk
}

type CCache struct {
	sync.RWMutex
	metricCache map[string]*CCacheMetric
}

func NewChunkCache() *CCache {
	return &CCache{
		metricCache: make(map[string]*CCacheMetric),
	}
}

func (c *CCache) Add(metric string, prev uint32, itergen iter.IterGen) error {
	c.RLock()
	if _, ok := c.metricCache[metric]; !ok {
		c.RUnlock()

		c.Lock()
		c.metricCache[metric] = &CCacheMetric{
			chunks: make(map[uint32]CacheChunk),
		}
		c.Unlock()
	}

	c.RLock()
	c.metricCache[metric].Add(prev, itergen)
	c.RUnlock()

	return nil
}

func (mc *CCacheMetric) Add(prev uint32, iter iter.IterGen) error {
	ts := iter.Ts()

	mc.RLock()
	if _, ok := mc.chunks[ts]; ok {
		mc.RUnlock()
		return nil
	}
	mc.RUnlock()

	mc.Lock()
	mc.chunks[ts] = CacheChunk{
		ts,
		0,
		prev,
		iter,
	}
	mc.Unlock()

	// if the previous chunk is cached, set this one as it's next
	mc.RLock()
	if _, ok := mc.chunks[prev]; ok {
		mc.chunks[prev].setNext(ts)
	}
	mc.RUnlock()

	return nil
}

// this assumes we have a lock
func (cc CacheChunk) setNext(next uint32) {
	cc.Next = next
}
