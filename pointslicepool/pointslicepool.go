package pointslicepool

import (
	"sync"

	"github.com/grafana/metrictank/schema"
)

// DefaultPointSliceSize is the default slice length to pull out of the pool for Get() calls. MUST match a size class.
// because we default maxDataPoints to 800, this seems like a sensible option
// might be worth experimenting with a smaller size class, and/or making classes and the default size configurable.
// i suspect many panels have 200<number of points<800. We may want to start smaller, and eat any append based [re]allocation should it occur
// (in that case at least we can save the result in an appropriate class)
const DefaultPointSliceSize = 1024

// PointSlicePool handles recycling of pointslicepools.
// internally it has several pools for different size classes
type PointSlicePool struct {
	defaultSize int
	sizes       [8]int
	pools       [8]sync.Pool
}

func New(defaultSize int) *PointSlicePool {
	p := PointSlicePool{
		defaultSize: defaultSize,

		// these are lower bounds. Every class contains slices of the exact bound capacity, or larger. Thus:
		// * On Get, the right class is the smallest size that is larger or equal than the given slice
		// * On Put, the right class for a given slice is the largest size that is equal or smaller than the given slice
		// why these? they're just a first stab at it.
		// too few size classes and Get() returns needlessly large slices
		// too many classes means you may allocate needlessly much (we could have useful slices, but they're in a higher size class)
		// perhaps the ideal is many finegrained classes, and upon GetMin(), try multiple classes as needed
		// we can also think about dynamically constructing size classes based on the real capacity/mincapacities we see at runtime
		sizes: [8]int{0, 32, 128, 1024, 4096, 32768, 262144, 2097152},
	}

	return &p
}

// Put puts the the slice in the appropriate size class
func (p *PointSlicePool) Put(s []schema.Point) {
	for i := len(p.sizes) - 1; i >= 0; i-- {
		if p.sizes[i] <= cap(s) {
			p.pools[i].Put(s[:0])
			return
		}
	}
}

func (p *PointSlicePool) Get() []schema.Point {
	return p.GetMin(p.defaultSize)
}

// GetMin returns a pointslice that has at least minCap capacity
func (p *PointSlicePool) GetMin(minCap int) []schema.Point {
	// find the smallest class that certainly has the right capacity
	class := len(p.sizes) - 1
	for i := 0; i < len(p.sizes); i++ {
		if p.sizes[i] >= minCap {
			class = i
			break
		}
	}
	candidate, ok := p.pools[class].Get().([]schema.Point)
	if ok {
		// this check is because the last class may return a slice that is too small
		if cap(candidate) >= minCap {
			return candidate
		}
		p.pools[class].Put(candidate)
	}
	return make([]schema.Point, 0, minCap)
}
