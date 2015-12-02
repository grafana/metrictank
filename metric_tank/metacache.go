package main

import (
	"sync"
)

type meta struct {
	interval   int
	targetType string
}

type MetaCache struct {
	intervals map[string]meta
	lock      sync.RWMutex
}

func NewMetaCache() *MetaCache {
	return &MetaCache{
		make(map[string]meta),
		sync.RWMutex{},
	}
}

func (mc *MetaCache) Add(key string, interval int, targetType string) {
	mc.lock.Lock()
	mc.intervals[key] = meta{interval, targetType}
	mc.lock.Unlock()
}

func (mc *MetaCache) Get(key string) meta {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	return mc.intervals[key]
}
