package mdata

import (
	"github.com/grafana/metrictank/internal/mdata/chunk/tsz"
	"github.com/grafana/metrictank/internal/schema"
)

type Result struct {
	// points not available in chunked form (from the ReOrderBuffer if enabled), in timestamp asc order.
	// these always go after points decoded from the iters.
	Points []schema.Point
	Iters  []tsz.Iter // chunked data from cache and/or from AggMetric (in time ascending order)
	Oldest uint32     // timestamp of oldest point we have, to know when and when not we may need to query slower storage
}
