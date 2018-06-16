package cache

import (
	"context"
	"errors"
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
	"gopkg.in/raintank/schema.v1"
)

var (
	maxSize         uint64
	searchFwdBug    = stats.NewCounter32("recovered_errors.cache.metric.searchForwardBug")
	ErrInvalidRange = errors.New("CCache: invalid range: from must be less than to")
)

func init() {
	flags := flag.NewFlagSet("chunk-cache", flag.ExitOnError)
	// (1024 ^ 3) * 4 = 4294967296 = 4G
	flags.Uint64Var(&maxSize, "max-size", 4294967296, "Maximum size of chunk cache in bytes. 0 disables cache")
	globalconf.Register("chunk-cache", flags)
}

type CCache struct {
	sync.RWMutex

	// one CCacheMetric struct per metric key, indexed by the key
	metricCache map[schema.AMKey]*CCacheMetric

	// sets of metric keys, indexed by their raw metric keys
	metricRawKeys map[schema.MKey]map[schema.Archive]struct{}

	// accounting for the cache. keeps track of when data needs to be evicted
	// and what should be evicted
	accnt accnt.Accnt

	// channel that's only used to signal go routines to stop
	stop chan interface{}

	tracer opentracing.Tracer
}

// NewCCache creates a new chunk cache.
// When cache is disabled, this will return nil
// but the caller doesn't have to worry about this and can call methods as usual on a nil cache
func NewCCache() *CCache {
	if maxSize == 0 {
		return nil
	}

	cc := &CCache{
		metricCache:   make(map[schema.AMKey]*CCacheMetric),
		metricRawKeys: make(map[schema.MKey]map[schema.Archive]struct{}),
		accnt:         accnt.NewFlatAccnt(maxSize),
		stop:          make(chan interface{}),
		tracer:        opentracing.NoopTracer{},
	}
	go cc.evictLoop()
	return cc
}

func (c *CCache) SetTracer(t opentracing.Tracer) {
	if c != nil {
		c.tracer = t
	}
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
func (c *CCache) DelMetric(rawMetric schema.MKey) (int, int) {
	if c == nil {
		return 0, 0
	}
	archives, series := 0, 0

	c.Lock()
	defer c.Unlock()

	archs, ok := c.metricRawKeys[rawMetric]
	if !ok {
		return archives, series
	}

	metric := schema.AMKey{MKey: rawMetric}
	for arch := range archs {
		metric.Archive = arch
		delete(c.metricCache, metric)
		c.accnt.DelMetric(metric)
		archives++
	}

	delete(c.metricRawKeys, rawMetric)
	series++

	return series, archives
}

// adds the given chunk to the cache, but only if the metric is sufficiently hot
func (c *CCache) CacheIfHot(metric schema.AMKey, prev uint32, itergen chunk.IterGen) {
	if c == nil {
		return
	}
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

func (c *CCache) Add(metric schema.AMKey, prev uint32, itergen chunk.IterGen) {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()

	ccm, ok := c.metricCache[metric]
	if !ok {
		ccm = NewCCacheMetric()
		ccm.Init(metric.MKey, prev, itergen)
		c.metricCache[metric] = ccm

		// if we do not have this raw key yet, create the entry with the association
		ccms, ok := c.metricRawKeys[metric.MKey]
		if !ok {
			c.metricRawKeys[metric.MKey] = map[schema.Archive]struct{}{
				metric.Archive: {},
			}
		} else {
			// otherwise, make sure the association exists
			ccms[metric.Archive] = struct{}{}
		}
	} else {
		ccm.Add(prev, itergen)
	}

	c.accnt.AddChunk(metric, itergen.Ts, itergen.Size())
}

func (cc *CCache) Reset() (int, int) {
	if cc == nil {
		return 0, 0
	}
	cc.Lock()
	cc.accnt.Reset()
	series := len(cc.metricRawKeys)
	archives := len(cc.metricCache)
	cc.metricCache = make(map[schema.AMKey]*CCacheMetric)
	cc.metricRawKeys = make(map[schema.MKey]map[schema.Archive]struct{})
	cc.Unlock()
	return series, archives
}

func (c *CCache) Stop() {
	if c == nil {
		return
	}
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
		delete(c.metricRawKeys[ccm.MKey], target.Metric.Archive)
		if len(c.metricRawKeys[ccm.MKey]) == 0 {
			delete(c.metricRawKeys, ccm.MKey)
		}
	}
}

// Search looks for the requested metric and returns a complete-as-possible CCSearchResult
// from is inclusive, until is exclusive
func (c *CCache) Search(ctx context.Context, metric schema.AMKey, from, until uint32) (*CCSearchResult, error) {
	if from >= until {
		return nil, ErrInvalidRange
	}

	res := &CCSearchResult{
		From:  from,
		Until: until,
	}

	if c == nil {
		accnt.CacheMetricMiss.Inc()
		return res, nil
	}

	ctx, span := tracing.NewSpan(ctx, c.tracer, "CCache.Search")
	defer span.Finish()

	c.RLock()
	defer c.RUnlock()

	cm, ok := c.metricCache[metric]
	if !ok {
		span.SetTag("cache", "miss")
		accnt.CacheMetricMiss.Inc()
		return res, nil
	}

	cm.Search(ctx, metric, res, from, until)
	if len(res.Start) == 0 && len(res.End) == 0 {
		span.SetTag("cache", "miss")
		accnt.CacheMetricMiss.Inc()
	} else {

		accnt.CacheChunkHit.Add(len(res.Start) + len(res.End))
		go func() {
			for _, hit := range res.Start {
				c.accnt.HitChunk(metric, hit.Ts)
			}
			for _, hit := range res.End {
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

	return res, nil
}
