package mdata

import "github.com/raintank/metrictank/consolidation"
import "github.com/raintank/metrictank/iter"

type Metrics interface {
	Get(key string) (Metric, bool)
	GetOrCreate(key string, partKey []byte) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
	Get(from, to uint32) (uint32, []iter.Iter)
	GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (uint32, []iter.Iter)
}
