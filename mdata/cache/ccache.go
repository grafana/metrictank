package cache

import (
	"context"
	"flag"
	"runtime"
	"sync"

	"github.com/grafana/metrictank/mdata/cache/accnt"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	maxSize      uint64
	searchFwdBug = stats.NewCounter32("recovered_errors.cache.metric.searchForwardBug")
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

	// sets of metric keys, indexed by their raw metric keys
	metricRawKeys map[string]map[string]struct{}

	// accounting for the cache. keeps track of when data needs to be evicted
	// and what should be evicted
	accnt accnt.Accnt

	// channel that's only used to signal go routines to stop
	stop chan interface{}

	tracer opentracing.Tracer
}

func NewCCache() *CCache {
	cc := &CCache{
		metricCache:   make(map[string]*CCacheMetric),
		metricRawKeys: make(map[string]map[string]struct{}),
		accnt:         accnt.NewFlatAccnt(maxSize),
		stop:          make(chan interface{}),
		tracer:        opentracing.NoopTracer{},
	}
	go cc.evictLoop()
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

// takes a raw key and deletes all archives associated with it from cache
func (c *CCache) DelMetric(rawMetric string) *CCDelMetricResult {
	res := &CCDelMetricResult{}

	c.Lock()
	defer c.Unlock()

	mets, ok := c.metricRawKeys[rawMetric]
	if !ok {
		return res
	}

	for met := range mets {
		delete(c.metricCache, met)
		c.accnt.DelMetric(met)
		res.Archives++
	}

	delete(c.metricRawKeys, rawMetric)
	res.Series++

	return res
}

// adds the given chunk to the cache, but only if the metric is sufficiently hot
func (c *CCache) CacheIfHot(metric string, prev uint32, itergen chunk.IterGen) {
	c.RLock()

	var met *CCacheMetric
	var ok bool

	// if this metric is not cached at all it is not hot
	if met, ok = c.metricCache[metric]; !ok {
		c.RUnlock()
		return
	}

	// if the previous chunk is not cached we consider the metric not hot enough to cache this chunk
	// only works reliably if the last chunk of that metric is span aware, otherwise lastTs() will be guessed
	// conservatively which means that the returned value will probably be lower than the real last ts
	if met.lastTs() < itergen.Ts {
		c.RUnlock()
		return
	}

	accnt.CacheChunkPushHot.Inc()

	c.RUnlock()
	met.Add(prev, itergen)
}

func (c *CCache) Add(metric, rawMetric string, prev uint32, itergen chunk.IterGen) {
	c.Lock()
	defer c.Unlock()

	ccm, ok := c.metricCache[metric]
	if !ok {
		ccm = NewCCacheMetric()
		ccm.Init(rawMetric, prev, itergen)
		c.metricCache[metric] = ccm

		// if we do not have this raw key yet, create an entry for it
		ccms, ok := c.metricRawKeys[rawMetric]
		if !ok {
			ccms = make(map[string]struct{})
			c.metricRawKeys[rawMetric] = ccms
		}

		// if we don't have it yet, associate the metric with this raw key
		if _, ok = ccms[metric]; !ok {
			ccms[metric] = struct{}{}
		}
	} else {
		ccm.Add(prev, itergen)
	}

	c.accnt.AddChunk(metric, itergen.Ts, itergen.Size())
}

func (cc *CCache) Reset() *CCDelMetricResult {
	res := &CCDelMetricResult{}
	cc.Lock()
	cc.accnt.Reset()
	res.Series = len(cc.metricRawKeys)
	res.Archives = len(cc.metricCache)
	cc.metricCache = make(map[string]*CCacheMetric)
	cc.metricRawKeys = make(map[string]map[string]struct{})
	cc.Unlock()
	return res
}

func (c *CCache) Stop() {
	c.accnt.Stop()
	c.stop <- nil
}

func (c *CCache) evict(target *accnt.EvictTarget) {
	c.Lock()
	// evict() might get called many times in a loop, but we don't want it to block
	// cache reads with the write lock, so we yield right after unlocking to allow
	// reads to go first.
	defer runtime.Gosched()
	defer c.Unlock()

	ccm, ok := c.metricCache[target.Metric]
	if !ok {
		return
	}

	log.Debug("CCache evict: evicting chunk %d on metric %s\n", target.Ts, target.Metric)
	length := c.metricCache[target.Metric].Del(target.Ts)
	if length == 0 {
		delete(c.metricCache, target.Metric)

		// this key should alway be present, if not there there is a corruption of the state
		delete(c.metricRawKeys[ccm.RawMetric], target.Metric)
		if len(c.metricRawKeys[ccm.RawMetric]) == 0 {
			delete(c.metricRawKeys, ccm.RawMetric)
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
