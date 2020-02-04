package keycache

import (
	"sync"
	"time"

	"github.com/grafana/metrictank/schema"
)

// KeyCache tracks for all orgs, which keys have been seen, and when was the last time
type KeyCache struct {
	staleThresh   Duration // number of 10-minutely periods
	pruneInterval time.Duration

	sync.RWMutex
	caches map[uint32]*Cache
}

// NewKeyCache creates a new KeyCache
func NewKeyCache(staleThresh, pruneInterval time.Duration) *KeyCache {
	if staleThresh.Hours() > 42 {
		panic("stale time may not exceed 42 hours due to resolution of internal bookkeeping")
	}
	if pruneInterval.Hours() > 42 {
		panic("prune interval may not exceed 42 hours due to resolution of internal bookkeeping")
	}
	if pruneInterval.Minutes() < 10 {
		panic("prune interval less than 10 minutes is useless due to resolution of internal bookkeeping")
	}
	k := &KeyCache{
		pruneInterval: pruneInterval,
		staleThresh:   Duration(int(staleThresh.Seconds()) / 600),
		caches:        make(map[uint32]*Cache),
	}
	go k.prune()
	return k
}

// Touch marks the key as seen and returns whether it was seen before
// callers should assure that t >= ref and t-ref <= 42 hours
func (k *KeyCache) Touch(key schema.MKey, t time.Time) bool {
	k.RLock()
	cache, ok := k.caches[key.Org]
	k.RUnlock()
	// most likely this branch won't execute
	if !ok {
		k.Lock()
		// check again in case another routine has just added it
		cache, ok = k.caches[key.Org]
		if !ok {
			cache = NewCache(NewRef(t))
			k.caches[key.Org] = cache
		}
		k.Unlock()
	}
	return cache.Touch(key.Key, t)
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

// prune makes sure each org's cache is pruned
func (k *KeyCache) prune() {
	tick := time.NewTicker(k.pruneInterval)
	for now := range tick.C {

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
			size := t.cache.Prune(now, k.staleThresh)
			if size == 0 {
				k.Lock()
				delete(k.caches, t.org)
				k.Unlock()
			}
		}
	}
}
