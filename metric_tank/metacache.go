package main

import (
	"sync"
)

type MetaCache struct {
	intervals map[string]int
	lock      sync.RWMutex
}

func NewMetaCache() *MetaCache {
	return &MetaCache{
		make(map[string]int),
		sync.RWMutex{},
	}
}

func (mc *MetaCache) Add(key string, interval int) {
	mc.lock.Lock()
	mc.intervals[key] = interval
	mc.lock.Unlock()
}

func (mc *MetaCache) Get(key string) int {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	return mc.intervals[key]
}
