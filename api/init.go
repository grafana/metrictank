package api

import (
	"sync"

	"github.com/grafana/metrictank/expr"
	"gopkg.in/raintank/schema.v1"
)

// default size is probably bigger than what most responses need, but it saves [re]allocations
// also it's possible that occasionnally more size is needed, causing a realloc of underlying array, and that extra space will stick around until next GC run.
const defaultPointSliceSize = 2000

var pointSlicePool sync.Pool

func pointSlicePoolAllocNew() interface{} {
	return make([]schema.Point, 0, defaultPointSliceSize)
}

func init() {
	pointSlicePool = sync.Pool{
		New: pointSlicePoolAllocNew,
	}
	expr.Pool(&pointSlicePool)
}
