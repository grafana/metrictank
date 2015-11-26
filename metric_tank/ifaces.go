package main

//import "github.com/dgryski/go-tsz"
import "github.com/raintank/go-tsz"

type Metrics interface {
	Get(key string) (Metric, bool)
	GetOrCreate(key string) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
	// the tsz implementation shouldn't leak through the abstraction, but this keeps it simple for now
	Get(from, to uint32) (uint32, []*tsz.Iter)
	GetAggregated(fn string, aggSpan, from, to uint32) (uint32, []*tsz.Iter)
}
