package api

import (
	"sync"

	"github.com/raintank/metrictank/expr"
	"gopkg.in/raintank/schema.v1"
)

const defaultPointSliceSize = 2000

var pointSlicePool sync.Pool

func init() {
	pointSlicePool = sync.Pool{
		// default size is probably bigger than what most responses need, but it saves [re]allocations
		// also it's possible that occasionnally more size is needed, causing a realloc of underlying array, and that extra space will stick around until next GC run.
		New: func() interface{} { return make([]schema.Point, 0, defaultPointSliceSize) },
	}
	expr.Pool(&pointSlicePool)
}
