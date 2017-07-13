package mdata

import (
	"github.com/raintank/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

type GetResult struct {
	Points []schema.Point
	Iters  []chunk.Iter
	Oldest uint32
}

type AddResult struct {
	Success bool           // defines whether adding the data point has been successful
	Flushed []schema.Point // data that's too old to be kept in the buffer, so it gets passed out
}
