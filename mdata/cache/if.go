package cache

import (
	"context"

	"github.com/grafana/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

type Cache interface {
	Add(metric, rawMetric schema.AMKey, prev uint32, itergen chunk.IterGen)
	CacheIfHot(metric schema.AMKey, prev uint32, itergen chunk.IterGen)
	Stop()
	Search(ctx context.Context, metric schema.AMKey, from, until uint32) *CCSearchResult
	DelMetric(rawMetric schema.AMKey) (int, int)
	Reset() (int, int)
}

type CachePusher interface {
	CacheIfHot(metric schema.AMKey, prev uint32, itergen chunk.IterGen)
}

type CCSearchResult struct {
	// if this result is Complete == false, then the following cassandra query
	// will need to use this value as from to fill in the missing data
	From uint32

	// just as with the above From, this will need to be used as the new until
	Until uint32

	// if Complete is true then the whole request can be served from cache
	Complete bool

	// if the cache contained the chunk containing the original "from" ts then
	// this slice will hold it as the first element, plus all the subsequent
	// cached chunks. If Complete is true then all chunks are in this slice.
	Start []chunk.IterGen

	// if complete is not true and the original "until" ts is in a cached chunk
	// then this slice will hold it as the first element, plus all the previous
	// ones in reverse order (because the search is seeking in reverse)
	End []chunk.IterGen
}
