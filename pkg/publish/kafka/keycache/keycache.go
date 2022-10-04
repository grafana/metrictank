package keycache

import (
	"sync"
	"time"

	schema "github.com/grafana/metrictank/schema"
)

// KeyCache tracks for all orgs, which keys have been seen, and when was the last time
type KeyCache struct {
	clearIdx      int // which shard should be cleared next?
	clearInterval time.Duration

	sync.RWMutex
	caches map[uint32]*Cache
}

// NewKeyCache creates a new KeyCache
// each clearInterval, all shards will be wiped
// (one at a time, spread out over the interval)
func NewKeyCache(clearInterval time.Duration) *KeyCache {
	k := &KeyCache{
		clearInterval: clearInterval / 256,
		caches:        make(map[uint32]*Cache),
	}
	go k.clear()
	return k
}

// Touch marks the key as seen and returns whether it was seen before
// callers should assure that t >= ref and t-ref <= 42 hours
func (k *KeyCache) Touch(key schema.MKey) bool {
	k.RLock()
	cache, ok := k.caches[key.Org]
	k.RUnlock()
	// most likely this branch won't execute
	if !ok {
		k.Lock()
		// check again in case another routine has just added it
		cache, ok = k.caches[key.Org]
		if !ok {
			cache = NewCache()
			k.caches[key.Org] = cache
		}
		k.Unlock()
	}
	return cache.Touch(key.Key)
}

// Len returns the size across all orgs
func (k *KeyCache) Len() int {
	var sum int
	k.RLock()
	caches := make([]*Cache, 0, len(k.caches))
	for _, c := range k.caches {
		caches = append(caches, c)
	}
	k.RUnlock()
	for _, c := range caches {
		sum += c.Len()
	}
	return sum
}

// clear makes sure each org's cache is periodically cleared
func (k *KeyCache) clear() {
	tick := time.NewTicker(k.clearInterval)
	for range tick.C {

		type target struct {
			org   uint32
			cache *Cache
		}

		k.RLock()
		targets := make([]target, 0, len(k.caches))
		for org, c := range k.caches {
			targets = append(targets, target{
				org,
				c,
			})
		}
		k.RUnlock()

		for _, t := range targets {
			size := t.cache.Clear(k.clearIdx)
			if size == 0 {
				k.Lock()
				delete(k.caches, t.org)
				k.Unlock()
			}
		}
		k.clearIdx += 1
		if k.clearIdx == 256 {
			k.clearIdx = 0
		}
	}
}
