package mdata

import "math"

// Aggregation is a container for all summary statistics / aggregated data for 1 metric, in 1 time frame
// if the cnt is 0, the numbers don't necessarily make sense.
type Aggregation struct {
	min float64
	max float64
	sum float64
	cnt float64
}

func NewAggregation() *Aggregation {
	return &Aggregation{
		min: math.MaxFloat64,
		max: -math.MaxFloat64,
	}
}

func (a *Aggregation) Add(val float64) {
	a.min = math.Min(val, a.min)
	a.max = math.Max(val, a.max)
	a.sum += val
	a.cnt += 1
}

func (a *Aggregation) Reset() {
	a.min = math.MaxFloat64
	a.max = -math.MaxFloat64
	a.sum = 0
	a.cnt = 0
}
