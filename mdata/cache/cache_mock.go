package cache

import (
	"context"
	"sync"

	"github.com/grafana/metrictank/mdata/chunk"
)

type MockCache struct {
	sync.Mutex
	AddCount        int
	CacheIfHotCount int
	CacheIfHotCb    func()
	StopCount       int
	SearchCount     int
	DelMetricRes    CCDelMetricResult
	DelMetricKeys   []string
}

func NewMockCache() *MockCache {
	return &MockCache{
		DelMetricKeys: make([]string, 0),
	}
}

func (mc *MockCache) Add(m, r string, t uint32, i chunk.IterGen) {
	mc.Lock()
	defer mc.Unlock()
	mc.AddCount++
}

func (mc *MockCache) CacheIfHot(m string, t uint32, i chunk.IterGen) {
	mc.Lock()
	defer mc.Unlock()
	mc.CacheIfHotCount++
	if mc.CacheIfHotCb != nil {
		mc.CacheIfHotCb()
	}
}

func (mc *MockCache) Stop() {
	mc.Lock()
	defer mc.Unlock()
	mc.StopCount++
}

func (mc *MockCache) Search(ctx context.Context, m string, f uint32, u uint32) *CCSearchResult {
	mc.Lock()
	defer mc.Unlock()
	mc.SearchCount++
	return nil
}

func (mc *MockCache) DelMetric(key string) *CCDelMetricResult {
	mc.DelMetricKeys = append(mc.DelMetricKeys, key)
	return &mc.DelMetricRes
}

func (mc *MockCache) Reset() *CCDelMetricResult {
	return &CCDelMetricResult{}
}
