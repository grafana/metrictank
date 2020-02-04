package keycache

import (
	"time"

	"github.com/grafana/metrictank/schema"
)

// Cache is a single-tenant keycache
// it is sharded for 2 reasons:
// * more granular GC (eg. less latency perceived by caller)
// * mild space savings cause keys are 1 byte shorter
// We shard on the first byte of the metric key, which we assume
// is evenly distributed.
type Cache struct {
	shards [256]Shard
}

// NewCache creates a new cache
func NewCache(ref Ref) *Cache {
	c := Cache{}
	for i := 0; i < 256; i++ {
		c.shards[i] = NewShard(ref)
	}
	return &c
}

// Touch marks the key as seen and returns whether it was seen before
// callers should assure that t >= ref and t-ref <= 42 hours
func (c *Cache) Touch(key schema.Key, t time.Time) bool {
	shard := int(key[0])
	return c.shards[shard].Touch(key, t)
}

// Len returns the length of the cache
func (c *Cache) Len() int {
	var sum int
	for i := 0; i < 256; i++ {
		sum += c.shards[i].Len()
	}
	return sum
}

// Prune makes sure all shards are pruned
func (c *Cache) Prune(now time.Time, staleThresh Duration) int {
	var remaining int
	for i := 0; i < 256; i++ {
		remaining += c.shards[i].Prune(now, staleThresh)
	}
	return remaining
}
