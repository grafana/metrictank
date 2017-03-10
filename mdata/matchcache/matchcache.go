package matchcache

import (
	"sync"
	"time"
)

// Cache caches key to uint16 lookups (for schemas and aggregations)
// when it cleans the cache it locks up the entire cache
// this is a tradeoff we can make for simplicity, since this sits on the ingestion
// path, where occasional stalls are ok.
type Cache struct {
	sync.Mutex
	data map[string]Item

	cleanInterval time.Duration
	expireAfter   time.Duration
}

type Item struct {
	val  uint16
	seen int64
}

func New(cleanInterval, expireAfter time.Duration) *Cache {
	m := &Cache{
		data:          make(map[string]Item),
		cleanInterval: cleanInterval,
		expireAfter:   expireAfter,
	}
	go m.maintain()
	return m
}

type AddFunc func(key string) uint16

// if not in cache, will atomically add it using the provided function
func (m *Cache) Get(key string, fn AddFunc) uint16 {
	m.Lock()
	item, ok := m.data[key]
	if !ok {
		item.val = fn(key)
	}
	item.seen = time.Now().Unix()
	m.data[key] = item
	m.Unlock()
	return item.val
}

func (m *Cache) maintain() {
	ticker := time.NewTicker(m.cleanInterval)
	diff := int64(m.expireAfter.Seconds())
	for now := range ticker.C {
		nowUnix := now.Unix()
		m.Lock()
		for key, item := range m.data {
			if nowUnix-item.seen > diff {
				delete(m.data, key)
			}
		}
		m.Unlock()
	}
}
