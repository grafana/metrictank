package mdata

import (
	"github.com/grafana/metrictank/mdata/chunk/tsz"
	"github.com/raintank/schema"
)

type Result struct {
	Points []schema.Point
	Iters  []tsz.Iter
	Oldest uint32 // timestamp of oldest point we have, to know when and when not we may need to query slower storage
}
