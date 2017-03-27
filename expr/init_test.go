package expr

import (
	"sync"

	"gopkg.in/raintank/schema.v1"
)

func init() {
	pointSlicePool = &sync.Pool{
		New: func() interface{} { return make([]schema.Point, 0, 100) },
	}
}
