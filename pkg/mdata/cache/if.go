package cache

import (
	"context"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/schema"
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
	IsCacheable(metric schema.AMKey) bool
	AddIfHot(metric schema.AMKey, prev uint32, itergen chunk.IterGen)
}

type CCSearchResult struct {
	Type ResultType

	// if Type is not Hit, then the following store query
	// will need to use this from value to fill in the missing data
	From uint32

	// if Type is not Hit, then the following store query
	// will need to use this until value to fill in the missing data
	Until uint32

	// if the cache contained the chunk containing the original "from" ts then
	// this slice will hold it as the first element, plus all the subsequent
	// cached chunks. If Type is Hit then all chunks are in this slice.
	Start []chunk.IterGen

	// if type is not Hit and the original "until" ts is in a cached chunk
	// then this slice will hold it as the first element, plus all the previous
	// ones in reverse order (because the search is seeking in reverse)
	End []chunk.IterGen
}

//go:generate stringer -type=ResultType
type ResultType uint8

const (
	Miss       ResultType = iota // no data for this request in cache
	HitPartial                   // request partially served from cache
	Hit                          // whole request served from cache
)
