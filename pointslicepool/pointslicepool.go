package pointslicepool

import (
	"sync"

	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stats"
)

var (

	// metric pointslicepool.ops.get-candidate.hit is how many times we could satisfy a get with a pointslice from the pool
	getCandHit = stats.NewCounterRate32("pointslicepool.ops.get-candidate.hit")

	// metric pointslicepool.ops.get-candidate.miss is how many times there was nothing in the pool to satisfy a get
	getCandMiss = stats.NewCounterRate32("pointslicepool.ops.get-candidate.miss")

	// metric pointslicepool.ops.get-candidate.unfit is how many times a pointslice from the pool was not large enough to satisfy a get
	getCandUnfit = stats.NewCounterRate32("pointslicepool.ops.get-candidate.unfit")

	// metric pointslicepool.ops.put.large is how many times a pointslice is added to the pool that is the same size or larger than the default
	putLarge = stats.NewCounterRate32("pointslicepool.ops.put.large")

	// metric pointslicepool.ops.put.small is how many times a pointslice is added to the pool that is smaller than the default
	putSmall = stats.NewCounterRate32("pointslicepool.ops.put.small")

	// metric pointslicepool.ops.get-make.large is how many times a pointslice is allocated that is larger or equal to the default size
	getMakeLarge = stats.NewCounterRate32("pointslicepool.ops.get-make.large")

	// metric pointslicepool.ops.get-make.small is how many times a pointslice is allocated that is smaller than the default size
	getMakeSmall = stats.NewCounterRate32("pointslicepool.ops.get-make.small")
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
	if cap(s) >= p.defaultSize {
		putLarge.Inc()
	} else {
		putSmall.Inc()
	}
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
			getCandHit.Inc()
			return candidate
		}
		getCandUnfit.Inc()
		p.p.Put(candidate)
	} else {
		getCandMiss.Inc()
	}
	if minCap >= p.defaultSize {
		getMakeLarge.Inc()
		return make([]schema.Point, 0, minCap)
	}
	// even if our caller needs a smaller cap now, we expect they will put it back in the pool
	// so it can later be reused.
	// may as well allocate a size now that we expect will be more useful down the road.
	getMakeSmall.Inc()
	return make([]schema.Point, 0, p.defaultSize)
}
