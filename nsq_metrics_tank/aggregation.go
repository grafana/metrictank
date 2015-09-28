package main

import "math"

// Aggregation is a container for all summary statistics / aggregated data for 1 metric, in 1 time frame
type Aggregation struct {
	min float64
	max float64
	sos float64
	sum float64
	cnt float64
}

func NewAggregation() *Aggregation {
	return &Aggregation{
		min: math.MaxFloat64,
	}
}

func (a *Aggregation) Add(ts uint32, val float64) {
	if val < a.min {
		a.min = val
	}
	if val > a.max {
		a.max = val
	}
	a.sos += math.Pow(val, 2)
	a.sum += val
	a.cnt += 1
}
func (a *Aggregation) Flush(ts uint32) {
	if a.cnt == 0 {
		// there was no data, so nothing to flush
	}
	// flush here
}
