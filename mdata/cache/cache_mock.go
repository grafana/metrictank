package cache

import (
	"github.com/raintank/metrictank/mdata/chunk"
)

type MockCache struct {
	adds      int
	addsIfHot int
	stops     int
	searches  int
	ops       chan func(mc *MockCache) // operations to execute serially
	hooks     []func() bool            // hooks to execute after each operation, e.g. after each potential state change
}

func NewMockCache() *MockCache {
	mc := &MockCache{
		ops: make(chan func(mc *MockCache), 0),
	}
	go mc.loop()
	return mc
}

func (mc *MockCache) loop() {
	for op := range mc.ops {
		op(mc)
		// execute hooks. when they return true they are "done" and can be removed
		for i, hook := range mc.hooks {
			if hook() {
				mc.hooks = append(mc.hooks[:i], mc.hooks[i+1:]...)
			}
		}
	}
}

func (mc *MockCache) Add(m string, t uint32, i chunk.IterGen) {
	mc.ops <- func(mc *MockCache) { mc.adds++ }
}

func (mc *MockCache) CacheIfHot(m string, t uint32, i chunk.IterGen) {
	mc.ops <- func(mc *MockCache) { mc.addsIfHot++ }
}

func (mc *MockCache) GetAddsIfHot() int {
	c := make(chan int)
	mc.ops <- func(mc *MockCache) { c <- mc.addsIfHot }
	return <-c
}

func (mc *MockCache) Stop() {
	mc.ops <- func(mc *MockCache) {
		close(mc.ops)
		mc.hooks = make([]func() bool, 0)
		mc.stops++
	}
}

func (mc *MockCache) Search(m string, f uint32, u uint32) *CCSearchResult {
	mc.ops <- func(mc *MockCache) { mc.searches++ }
	return nil
}

func (mc *MockCache) AfterAddsIfHot(addsIfHot int) chan struct{} {
	ret := make(chan struct{})
	mc.ops <- func(mc *MockCache) {
		mc.hooks = append(mc.hooks, func() bool {
			if mc.addsIfHot >= addsIfHot {
				ret <- struct{}{}
				return true
			}
			return false
		})
	}
	return ret
}
