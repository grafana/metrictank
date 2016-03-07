package batch

// aggregation functions for batches of data
import (
	"math"
)

type AggFunc func(in []float64) float64

func Avg(in []float64) float64 {
	if len(in) == 0 {
		panic("avg() called in aggregator with 0 terms")
	}
	return Sum(in) / Cnt(in)
}

func Cnt(in []float64) float64 {
	usable := Usable(in)
	if len(usable) == 0 {
		return math.NaN()
	}
	return float64(len(usable))
}

func Min(in []float64) float64 {
	if len(in) == 0 {
		panic("min() called in aggregator with 0 terms")
	}
	usable := Usable(in)
	if len(usable) == 0 {
		return math.NaN()
	}
	min := math.Inf(1)
	for _, v := range usable {
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
	usable := Usable(in)
	if len(usable) == 0 {
		return math.NaN()
	}
	max := math.Inf(-1)
	for _, v := range usable {
		if v > max {
			max = v
		}
	}
	return max
}

func Sum(in []float64) float64 {
	usable := Usable(in)
	if len(usable) == 0 {
		return math.NaN()
	}
	sum := float64(0)
	for _, term := range usable {
		sum += term
	}
	return sum
}

func Usable(in []float64) []float64 {
	u := make([]float64, 0)
	for _, v := range in {
		if !math.IsNaN(v) {
			u = append(u, v)
		}
	}
	return u
}
