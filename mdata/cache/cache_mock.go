package cache

import (
	"context"
	"sync"

	"github.com/grafana/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
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
	DelMetricKeys     []schema.MKey
	ResetCalls        int
}

func NewMockCache() *MockCache {
	return &MockCache{}
}

func (mc *MockCache) Add(metric schema.AMKey, prev uint32, itergen chunk.IterGen) {
	mc.Lock()
	defer mc.Unlock()
	mc.AddCount++
}

func (mc *MockCache) AddRange(metric schema.AMKey, prev uint32, itergens []chunk.IterGen) {
	mc.Lock()
	defer mc.Unlock()
	mc.AddCount += len(itergens)
}

func (mc *MockCache) CacheIfHot(metric schema.AMKey, prev uint32, itergen chunk.IterGen) {
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

func (mc *MockCache) Search(ctx context.Context, metric schema.AMKey, from uint32, until uint32) (*CCSearchResult, error) {
	mc.Lock()
	defer mc.Unlock()
	mc.SearchCount++
	return nil, nil
}

func (mc *MockCache) DelMetric(rawMetric schema.MKey) (int, int) {
	mc.DelMetricKeys = append(mc.DelMetricKeys, rawMetric)
	return mc.DelMetricSeries, mc.DelMetricArchives
}

func (mc *MockCache) Reset() (int, int) {
	mc.ResetCalls++
	return mc.DelMetricSeries, mc.DelMetricArchives
}
