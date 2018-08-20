package cache

import (
	"context"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/raintank/schema"
)

type Cache interface {
	Add(metric schema.AMKey, prev uint32, itergen chunk.IterGen)
	AddIfHot(metric schema.AMKey, prev uint32, itergen chunk.IterGen)
	AddRange(metric schema.AMKey, prev uint32, itergens []chunk.IterGen)
	Stop()
	Search(ctx context.Context, metric schema.AMKey, from, until uint32) (*CCSearchResult, error)
	DelMetric(rawMetric schema.MKey) (int, int)
	Reset() (int, int)
}

type CachePusher interface {
	AddIfHot(metric schema.AMKey, prev uint32, itergen chunk.IterGen)
}

type CCSearchResult struct {
	// whether the whole request can be served from cache
	Complete bool

	// if this result is not Complete, then the following store query
	// will need to use this from value to fill in the missing data
	From uint32

	// if this result is not Complete, then the following store query
	// will need to use this until value to fill in the missing data
	Until uint32

	// if the cache contained the chunk containing the original "from" ts then
	// this slice will hold it as the first element, plus all the subsequent
	// cached chunks. If Complete is true then all chunks are in this slice.
	Start []chunk.IterGen

	// if this result is not Complete and the original "until" ts is in a cached chunk
	// then this slice will hold it as the first element, plus all the previous
	// ones in reverse order (because the search is seeking in reverse)
	End []chunk.IterGen
}
