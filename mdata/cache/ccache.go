package cache

import (
	"context"
	"flag"
	"runtime"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/raintank/metrictank/mdata/cache/accnt"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/metrictank/tracing"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	maxSize        uint64
	cacheMetricBug = stats.NewCounter32("cache.ops.metric.searchForward-bug-surpressed")
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

	// channel that's only used to signal go routines to stop
	stop chan interface{}

	tracer opentracing.Tracer

	addQ chan AddTarget
}

type AddTarget struct {
	Metric string
	PrevTs uint32
	Itgen  chunk.IterGen
	IfHot  bool
}

func NewCCache() *CCache {
	cc := &CCache{
		metricCache: make(map[string]*CCacheMetric),
		accnt:       accnt.NewFlatAccnt(maxSize),
		stop:        make(chan interface{}),
		tracer:      opentracing.NoopTracer{},
		addQ:        make(chan AddTarget, 100),
	}
	go cc.evictLoop()
	go cc.addLoop()
	return cc
}

func (c *CCache) SetTracer(t opentracing.Tracer) {
	c.tracer = t
}

func (c *CCache) evictLoop() {
	evictQ := c.accnt.GetEvictQ()
	for {
		select {
		case target := <-evictQ:
			c.evict(target)
		case _ = <-c.stop:
			return
		}
	}
}

func (c *CCache) addLoop() {
	bufferSize := 100
	buffer := make([]AddTarget, bufferSize)
	tick := time.Tick(time.Duration(1) * time.Second)
	for {
		select {
		case <-tick:
			if len(buffer) > 0 {
				c.add(buffer)
				buffer = buffer[:0]
			}
		case target := <-c.addQ:
			buffer = append(buffer, target)
			if len(buffer) > bufferSize {
				c.add(buffer)
				buffer = buffer[:0]
			}
		case _ = <-c.stop:
			return
		}
	}
}

func (c *CCache) isHot(metric string, ts uint32) bool {
	var met *CCacheMetric
	var ok bool

	// if this metric is not cached at all it is not hot
	if met, ok = c.metricCache[metric]; !ok {
		c.RUnlock()
		return false
	}

	// if the previous chunk is not cached we consider the metric not hot enough to cache this chunk
	// only works reliably if the last chunk of that metric is span aware, otherwise lastTs() will be guessed
	// conservatively which means that the returned value will probably be lower than the real last ts
	if met.lastTs() < ts {
		return false
	}

	accnt.CacheChunkPushHot.Inc()

	return true
}

// adds the given chunk to the cache, but only if the metric is sufficiently hot
func (c *CCache) CacheIfHot(metric string, prev uint32, itergen chunk.IterGen) {
	c.addQ <- AddTarget{
		Metric: metric,
		PrevTs: prev,
		Itgen:  itergen,
		IfHot:  true,
	}
}

func (c *CCache) Add(metric string, prev uint32, itergen chunk.IterGen) {
	c.addQ <- AddTarget{
		Metric: metric,
		PrevTs: prev,
		Itgen:  itergen,
		IfHot:  false,
	}
}

func (c *CCache) add(targets []AddTarget) {
	currentLock := 0 // 1 = read, 2 = write

	for _, target := range targets {
		if target.IfHot {
			if currentLock != 1 {
				if currentLock == 2 {
					c.Unlock()
				}
				c.RLock()
				currentLock = 1
			}

			if !c.isHot(target.Metric, target.Itgen.Ts) {
				continue
			}
		}

		if currentLock != 2 {
			if currentLock == 1 {
				c.RUnlock()
			}
			c.Lock()
			currentLock = 2
		}

		if ccm, ok := c.metricCache[target.Metric]; !ok {
			ccm = NewCCacheMetric()
			ccm.Init(target.PrevTs, target.Itgen)
			c.metricCache[target.Metric] = ccm
		} else {
			ccm.Add(target.PrevTs, target.Itgen)
		}

		c.accnt.AddChunk(target.Metric, target.Itgen.Ts, target.Itgen.Size())
	}

	if currentLock == 1 {
		c.RUnlock()
	} else if currentLock == 2 {
		c.Unlock()
	}
}

func (cc *CCache) Reset() {
	cc.accnt.Reset()
	cc.Lock()
	cc.metricCache = make(map[string]*CCacheMetric)
	cc.Unlock()
}

func (c *CCache) Stop() {
	c.accnt.Stop()
	c.stop <- nil
	c.stop <- nil
}

func (c *CCache) evict(target *accnt.EvictTarget) {
	c.Lock()
	// evict() might get called many times in a loop, but we don't want it to block
	// cache reads with the write lock, so we yield right after unlocking to allow
	// reads to go first.
	defer runtime.Gosched()
	defer c.Unlock()

	if _, ok := c.metricCache[target.Metric]; ok {
		log.Debug("CCache evict: evicting chunk %d on metric %s\n", target.Ts, target.Metric)
		length := c.metricCache[target.Metric].Del(target.Ts)
		if length == 0 {
			delete(c.metricCache, target.Metric)
		}
	}
}

func (c *CCache) Search(ctx context.Context, metric string, from, until uint32) *CCSearchResult {
	ctx, span := tracing.NewSpan(ctx, c.tracer, "CCache.Search")
	defer span.Finish()
	var hit chunk.IterGen
	var cm *CCacheMetric
	var ok bool
	res := &CCSearchResult{
		From:  from,
		Until: until,
	}

	if from == until {
		return res
	}

	c.RLock()
	defer c.RUnlock()

	if cm, ok = c.metricCache[metric]; !ok {
		span.SetTag("cache", "miss")
		accnt.CacheMetricMiss.Inc()
		return res
	}

	cm.Search(ctx, metric, res, from, until)
	if len(res.Start) == 0 && len(res.End) == 0 {
		span.SetTag("cache", "miss")
		accnt.CacheMetricMiss.Inc()
	} else {

		accnt.CacheChunkHit.Add(len(res.Start) + len(res.End))
		go func() {
			for _, hit = range res.Start {
				c.accnt.HitChunk(metric, hit.Ts)
			}
			for _, hit = range res.End {
				c.accnt.HitChunk(metric, hit.Ts)
			}
		}()

		if res.Complete {
			span.SetTag("cache", "hit-full")
			accnt.CacheMetricHitFull.Inc()
		} else {
			span.SetTag("cache", "hit-partial")
			accnt.CacheMetricHitPartial.Inc()
		}
	}

	return res
}
