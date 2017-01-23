package accnt

import "github.com/raintank/metrictank/stats"

var (

	// metric cache.ops.metric.hit-full is how many metrics were hit fully (all needed chunks in cache)
	CacheMetricHitFull = stats.NewCounter32("cache.ops.metric.hit-full")

	// metric cache.ops.metric.hit-partial is how many metrics were hit partially (some of the needed chunks in cache, but not all)
	CacheMetricHitPartial = stats.NewCounter32("cache.ops.metric.hit-partial")

	// metric cache.ops.metric.miss is how many metrics were missed fully (no needed chunks in cache)
	CacheMetricMiss = stats.NewCounter32("cache.ops.metric.miss")

	// metric cache.ops.metric.add is how many metrics were added to the cache
	cacheMetricAdd = stats.NewCounter32("cache.ops.metric.add")

	// metric cache.ops.metric.evict is how many metrics were evicted from the cache
	cacheMetricEvict = stats.NewCounter32("cache.ops.metric.evict")

	// metric cache.ops.chunk.hit is how many chunks were hit
	CacheChunkHit = stats.NewCounter32("cache.ops.chunk.hit")

	// metric cache.ops.chunk.push-hot is how many chunks have been pushed into the cache because their metric is hot
	CacheChunkPushHot = stats.NewCounter32("cache.ops.chunk.push-hot")

	// metric cache.ops.chunk.add is how many chunks were added to the cache
	cacheChunkAdd = stats.NewCounter32("cache.ops.chunk.add")

	// metric cache.ops.chunk.evict is how many chunks were evicted from the cache
	cacheChunkEvict = stats.NewCounter32("cache.ops.chunk.evict")

	cacheSizeMax  = stats.NewGauge64("cache.size.max")
	cacheSizeUsed = stats.NewGauge64("cache.size.used")
)
