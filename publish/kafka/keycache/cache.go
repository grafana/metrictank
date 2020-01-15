package keycache

import schema "github.com/grafana/metrictank/schema"

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
func NewCache() *Cache {
	c := Cache{}
	for i := 0; i < 256; i++ {
		c.shards[i] = NewShard()
	}
	return &c
}

// Touch marks the key as seen and returns whether it was seen before
func (c *Cache) Touch(key schema.Key) bool {
	shard := int(key[0])
	return c.shards[shard].Touch(key)
}

// Len returns the length of the cache
func (c *Cache) Len() int {
	var sum int
	for i := 0; i < 256; i++ {
		sum += c.shards[i].Len()
	}
	return sum
}

// Clear resets the given shard
func (c *Cache) Clear(i int) int {
	c.shards[i].Reset()
	return c.Len()
}
