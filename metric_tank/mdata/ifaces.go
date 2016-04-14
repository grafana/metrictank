package mdata

import "github.com/raintank/raintank-metric/metric_tank/consolidation"
import "github.com/raintank/raintank-metric/metric_tank/iter"

type Metrics interface {
	Get(key string) (Metric, bool)
	GetOrCreate(key string) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
	Get(from, to uint32) (uint32, []iter.Iter)
	GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (uint32, []iter.Iter)
}
