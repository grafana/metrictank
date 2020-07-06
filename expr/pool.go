package expr

import (
	"sync"

	"github.com/grafana/metrictank/schema"
)

var pointSlicePool *sync.Pool

// Pool tells the expr library which pool to use for temporary []schema.Point
// this lets the expr package effectively create and drop point slices as needed
// it is recommended you use the same pool in your application, e.g. to get slices
// when loading the initial data, and to return the buffers back to the pool once
// the output from this package's processing is no longer needed.
func Pool(p *sync.Pool) {
	pointSlicePool = p
}

// pointSlicePoolGet returns a pointslice of at least minCap capacity.
// similar code lives also in api.Fix(). at some point we should really clean up our pool code.
func pointSlicePoolGet(minCap int) []schema.Point {
	candidate := pointSlicePool.Get().([]schema.Point)
	if cap(candidate) >= minCap {
		return candidate
	}
	pointSlicePool.Put(candidate)
	return make([]schema.Point, 0, minCap)
}
