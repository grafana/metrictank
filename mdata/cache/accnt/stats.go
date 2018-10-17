package accnt

import "github.com/grafana/metrictank/stats"

var (

	// metric cache.ops.metric.hit-full is how many metrics were hit fully (all needed chunks in cache)
	CacheMetricHitFull = stats.NewCounterRate32("cache.ops.metric.hit-full")

	// metric cache.ops.metric.hit-partial is how many metrics were hit partially (some of the needed chunks in cache, but not all)
	CacheMetricHitPartial = stats.NewCounterRate32("cache.ops.metric.hit-partial")

	// metric cache.ops.metric.miss is how many metrics were missed fully (no needed chunks in cache)
	CacheMetricMiss = stats.NewCounterRate32("cache.ops.metric.miss")

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

	// metric cache.size.max is the maximum size of the cache (overhead does not count towards this limit)
	cacheSizeMax = stats.NewGauge64("cache.size.max")

	// metric cache.size.used is how much of the cache is used (sum of the chunk data without overhead)
	cacheSizeUsed = stats.NewGauge64("cache.size.used")

	// metric cache.overhead.chunk is an approximation of the overhead used to store chunks in the cache
	cacheOverheadChunk = stats.NewGauge64("cache.overhead.chunk")

	// metric cache.overhead.flat is an approximation of the overhead used by flat accounting
	cacheOverheadFlat = stats.NewGauge64("cache.overhead.flat")

	// metric cache.overhead.lru is an approximation of the overhead used by the LRU
	cacheOverheadLru = stats.NewGauge64("cache.overhead.lru")

	accntEventAddDuration = stats.NewLatencyHistogram15s32("cache.accounting.queue.add")
	accntEventQueueUsed   = stats.NewRange32("cache.accounting.queue.size.used")
	accntEventQueueMax    = stats.NewGauge64("cache.accounting.queue.size.max")
)
