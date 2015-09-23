package main

type Metrics interface {
	Get(key string) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
}
