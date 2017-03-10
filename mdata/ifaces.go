package mdata

import (
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/mdata/chunk"
)

type Metrics interface {
	Get(key string) (Metric, bool)
	GetOrCreate(key, name string, schemaI, aggI uint16) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
	Get(from, to uint32) (uint32, []chunk.Iter)
	GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (uint32, []chunk.Iter)
}
