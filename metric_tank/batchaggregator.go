package main

// aggregation functions for batches of data
import (
	"math"
)

//go:generate stringer -type=aggFunc

type aggregator int

const (
	none aggregator = iota
	avg
	last
	min
	max
	sum
)

func (foo aggregator) String() string {
	switch aggregator {
	case last:
		return "last"
	case min:
		return "min"
	case max:
		return "max"
	case sum:
		return "sum"
	}
	panic("unknown aggregator to String()")
}

type aggFunc func(in []float64) float64

func Avg(in []float64) float64 {
	if len(in) == 0 {
		panic("avg() called in aggregator with 0 terms")
	}
	return Sum(in) / float64(len(in))
}

func Last(in []float64) float64 {
	if len(in) == 0 {
		panic("last() called in aggregator with 0 terms")
	}
	return in[len(in)-1]
}

func Min(in []float64) float64 {
	if len(in) == 0 {
		panic("min() called in aggregator with 0 terms")
	}
	min := math.MaxFloat64
	for _, v := range in {
		if v < min {
			min = v
		}
	}
	return min
}

func Max(in []float64) float64 {
	if len(in) == 0 {
		panic("max() called in aggregator with 0 terms")
	}
	max := float64(0)
	for _, v := range in {
		if v > max {
			max = v
		}
	}
	return max
}

func Sum(in []float64) float64 {
	sum := float64(0)
	for _, term := range in {
		sum += term
	}
	return sum
}

func getAggFunc(aggregator aggregator) aggFunc {
	var consFunc aggFunc
	switch aggregator {
	case avg:
		consFunc = Avg
	case last:
		consFunc = Last
	case min:
		consFunc = Min
	case max:
		consFunc = Max
	case sum:
		consFunc = Sum
	}
	return consFunc
}
