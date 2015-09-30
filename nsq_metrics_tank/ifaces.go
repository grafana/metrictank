package main

import "github.com/dgryski/go-tsz"

type Metrics interface {
	Get(key string) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
	// the tsz implementation shouldn't leak through the abstraction, but this keeps it simple for now
	GetSafe(from, to uint32) []*tsz.Iter
	GetUnsafe(from, to uint32) (uint32, []*tsz.Iter, error)
}
