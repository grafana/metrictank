package mdata

import (
	"github.com/raintank/metrictank/consolidation"
)

type Metrics interface {
	Get(key string) (Metric, bool)
	GetOrCreate(key, name string, schemaId, aggId uint16) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
	Get(from, to uint32) GetResult
	GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) GetResult
}
