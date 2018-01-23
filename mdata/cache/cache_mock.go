package cache

import (
	"context"
	"sync"

	"github.com/grafana/metrictank/mdata/chunk"
)

type MockCache struct {
	sync.Mutex
	AddCount          int
	CacheIfHotCount   int
	CacheIfHotCb      func()
	StopCount         int
	SearchCount       int
	DelMetricArchives int
	DelMetricSeries   int
	DelMetricKeys     []string
	ResetCalls        int
}

func NewMockCache() *MockCache {
	return &MockCache{}
}

func (mc *MockCache) Add(metric, rawMetric string, prev uint32, itergen chunk.IterGen) {
	mc.Lock()
	defer mc.Unlock()
	mc.AddCount++
}

func (mc *MockCache) CacheIfHot(metric string, prev uint32, itergen chunk.IterGen) {
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

func (mc *MockCache) Search(ctx context.Context, metric string, from uint32, until uint32) *CCSearchResult {
	mc.Lock()
	defer mc.Unlock()
	mc.SearchCount++
	return nil
}

func (mc *MockCache) DelMetric(rawMetric string) (int, int) {
	mc.DelMetricKeys = append(mc.DelMetricKeys, rawMetric)
	return mc.DelMetricSeries, mc.DelMetricArchives
}

func (mc *MockCache) Reset() (int, int) {
	mc.ResetCalls++
	return mc.DelMetricSeries, mc.DelMetricArchives
}
