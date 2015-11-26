package main

// aggregation functions for batches of data
import (
	"math"
)

type aggregator int

const (
	avg aggregator = iota
	last
	min
	max
	sum
)

type aggFunc func(in []float64) float64

// TODO not sure yet if any of these funcs will be used
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
