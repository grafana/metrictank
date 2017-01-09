package cache

import (
	"github.com/raintank/metrictank/mdata/chunk"
	"sync"
)

type MockCache struct {
	sync.Mutex
	AddCount        int
	CacheIfHotCount int
	CacheIfHotCb    func()
	StopCount       int
	SearchCount     int
}

func (mc *MockCache) Add(m string, t uint32, i chunk.IterGen) {
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

func (mc *MockCache) Search(m string, f uint32, u uint32) *CCSearchResult {
	mc.Lock()
	defer mc.Unlock()
	mc.SearchCount++
	return nil
}
