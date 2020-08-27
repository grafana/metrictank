package pointslicepool

import (
	"sync"

	"github.com/grafana/metrictank/schema"
)

// default size is probably bigger than what most responses need, but it saves [re]allocations
// also it's possible that occasionally more size is needed, causing a realloc of underlying array, and that extra space will stick around until next GC run.
const DefaultPointSliceSize = 2000

type PointSlicePool struct {
	defaultSize int
	p           sync.Pool
}

func New(defaultSize int) *PointSlicePool {
	return &PointSlicePool{
		defaultSize: defaultSize,
		p:           sync.Pool{},
	}
}

func (p *PointSlicePool) Put(s []schema.Point) {
	p.p.Put(s[:0])
}

func (p *PointSlicePool) Get() []schema.Point {
	return p.GetMin(p.defaultSize)
}

// GetMin returns a pointslice that has at least minCap capacity
func (p *PointSlicePool) GetMin(minCap int) []schema.Point {
	candidate, ok := p.p.Get().([]schema.Point)
	if ok {
		if cap(candidate) >= minCap {
			return candidate
		}
		p.p.Put(candidate)
	}
	if minCap > p.defaultSize {
		return make([]schema.Point, 0, minCap)
	}
	// even if our caller needs a smaller cap now, we expect they will put it back in the pool
	// so it can later be reused.
	// may as well allocate a size now that we expect will be more useful down the road.
	return make([]schema.Point, 0, p.defaultSize)
}
