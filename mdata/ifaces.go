package mdata

import (
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

type Metrics interface {
	Get(key string) (Metric, bool)
	GetOrCreate(key, name string, schemaId, aggId uint16) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
	Get(from, to uint32) MetricResult
	GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) MetricResult
}

type MetricResult struct {
	Raw    []schema.Point
	Iters  []chunk.Iter
	Oldest uint32
}
