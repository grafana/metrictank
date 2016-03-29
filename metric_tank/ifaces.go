package main

import "github.com/raintank/raintank-metric/metric_tank/consolidation"

type Metrics interface {
	Get(key string) (Metric, bool)
	GetOrCreate(key string) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
	Get(from, to uint32) (uint32, []Iter)
	GetAggregated(consolidator consolidation.Consolidator, aggSpan uint16, from, to uint32) (uint32, []Iter)
}
