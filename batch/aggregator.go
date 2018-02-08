// Package batch implements batched processing for slices of points
// in particular aggregations
package batch

// aggregation functions for batches of data
import (
	"gopkg.in/raintank/schema.v1"
	"math"
)

type AggFunc func(in []schema.Point) float64

func Avg(in []schema.Point) float64 {
	valid := float64(0)
	sum := float64(0)
	for _, term := range in {
		if !math.IsNaN(term.Val) {
			valid += 1
			sum += term.Val
		}
	}
	if valid == 0 {
		return math.NaN()
	}
	return sum / valid
}

func Cnt(in []schema.Point) float64 {
	valid := float64(0)
	for _, v := range in {
		if !math.IsNaN(v.Val) {
			valid += 1
		}
	}
	if valid == 0 {
		return math.NaN()
	}
	return valid
}

func Lst(in []schema.Point) float64 {
	lst := math.NaN()
	for _, v := range in {
		if !math.IsNaN(v.Val) {
			lst = v.Val
		}
	}
	return lst
}

func Min(in []schema.Point) float64 {
	valid := false
	min := math.Inf(1)
	for _, v := range in {
		if !math.IsNaN(v.Val) {
			valid = true
			if v.Val < min {
				min = v.Val
			}
		}
	}
	if !valid {
		min = math.NaN()
	}
	return min
}

func Max(in []schema.Point) float64 {
	valid := false
	max := math.Inf(-1)
	for _, v := range in {
		if !math.IsNaN(v.Val) {
			valid = true
			if v.Val > max {
				max = v.Val
			}
		}
	}
	if !valid {
		max = math.NaN()
	}
	return max
}

func Sum(in []schema.Point) float64 {
	valid := false
	sum := float64(0)
	for _, term := range in {
		if !math.IsNaN(term.Val) {
			valid = true
			sum += term.Val
		}
	}
	if !valid {
		sum = math.NaN()
	}
	return sum
}
