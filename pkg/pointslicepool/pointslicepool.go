package pointslicepool

import (
	"sync"

	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/util"
)

type PointSlicePool struct {
	getCandHit     *stats.CounterRate32
	getCandMiss    *stats.CounterRate32
	getCandUnfit   *stats.CounterRate32
	putLarge       *stats.CounterRate32
	putDefault     *stats.CounterRate32
	putSmall       *stats.CounterRate32
	getMakeLarge   *stats.CounterRate32
	getMakeDefault *stats.CounterRate32
	getMakeSmall   *stats.CounterRate32
	defaultSize    int
	p              sync.Pool
}

func New(defaultSize int) *PointSlicePool {
	return &PointSlicePool{
		// metric pointslicepool.ops.get-candidate.hit is how many times we could satisfy a get with a pointslice from the pool
		getCandHit: stats.NewCounterRate32("pointslicepool.ops.get-candidate.hit"),
		// metric pointslicepool.ops.get-candidate.miss is how many times there was nothing in the pool to satisfy a get
		getCandMiss: stats.NewCounterRate32("pointslicepool.ops.get-candidate.miss"),
		// metric pointslicepool.ops.get-candidate.unfit is how many times a pointslice from the pool was not large enough to satisfy a get
		getCandUnfit: stats.NewCounterRate32("pointslicepool.ops.get-candidate.unfit"),
		// metric pointslicepool.ops.put.large is how many times a pointslice is added to the pool that is larger than the default
		putLarge: stats.NewCounterRate32("pointslicepool.ops.put.large"),
		// metric pointslicepool.ops.put.default is how many times a pointslice is added to the pool that is equal to the default
		putDefault: stats.NewCounterRate32("pointslicepool.ops.put.default"),
		// metric pointslicepool.ops.put.small is how many times a pointslice is added to the pool that is smaller than the default
		putSmall: stats.NewCounterRate32("pointslicepool.ops.put.small"),
		// metric pointslicepool.ops.get-make.large is how many times a pointslice is allocated that is larger than the default size
		getMakeLarge: stats.NewCounterRate32("pointslicepool.ops.get-make.large"),
		// metric pointslicepool.ops.get-make.default is how many times a pointslice is allocated that is equal to the default size
		getMakeDefault: stats.NewCounterRate32("pointslicepool.ops.get-make.default"),
		// metric pointslicepool.ops.get-make.small is how many times a pointslice is allocated that is smaller than the default size
		getMakeSmall: stats.NewCounterRate32("pointslicepool.ops.get-make.small"),
		defaultSize:  defaultSize,
		p:            sync.Pool{},
	}
}

// SetDefaultSize - Change the default size for the point slice pool.
// This function is not thread-safe and should only be called when pool is not being actively used.
func (p *PointSlicePool) SetDefaultSize(size int) {
	p.defaultSize = size
}

func (p *PointSlicePool) PutMaybeNil(s []schema.Point) {
	if s != nil {
		p.Put(s)
	}
}

func (p *PointSlicePool) Put(s []schema.Point) {
	p.incrementCounter(cap(s), p.putLarge, p.putDefault, p.putSmall)
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
			p.getCandHit.Inc()
			return candidate
		}
		p.getCandUnfit.Inc()
		p.p.Put(candidate)
	} else {
		p.getCandMiss.Inc()
	}

	p.incrementCounter(minCap, p.getMakeLarge, p.getMakeDefault, p.getMakeSmall)

	// even if our caller needs a smaller cap now, we expect they will put it back in the pool
	// so it can later be reused.
	// may as well allocate a size now that we expect will be more useful down the road.
	allocCap := util.MaxInt(minCap, p.defaultSize)
	return make([]schema.Point, 0, allocCap)
}

func (p *PointSlicePool) incrementCounter(size int, large, deflt, small *stats.CounterRate32) {
	if size > p.defaultSize {
		large.Inc()
	} else if size == p.defaultSize {
		deflt.Inc()
	} else {
		small.Inc()
	}
}
