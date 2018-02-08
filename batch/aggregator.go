// Package batch implements batched processing for slices of points
// in particular aggregations
package batch

// aggregation functions for batches of data
import (
	"gopkg.in/raintank/schema.v1"
	"math"
	"sort"
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

func Mult(in []schema.Point) float64 {
	valid := false
	mult := float64(1)
	for _, term := range in {
		if math.IsNaN(term.Val) {
			// NaN * anything equals NaN()
			mult = math.NaN()
			break
		}
		valid = true
		mult *= term.Val
	}
	if !valid {
		mult = math.NaN()
	}
	return mult
}

func Med(in []schema.Point) float64 {
	med := math.NaN()
	vals := make([]float64, 0, len(in))
	for i := 0; i < len(in); i++ {
		p := in[i].Val
		if !math.IsNaN(p) {
			vals = append(vals, p)
		}
	}
	if len(vals) != 0 {
		sort.Float64s(vals)
		mid := len(vals) / 2
		if len(vals)%2 == 0 {
			med = (vals[mid-1] + vals[mid]) / 2
		} else {
			med = vals[mid]
		}
	}
	return med
}

func Diff(in []schema.Point) float64 {
	diff := math.NaN()
	for i := 0; i < len(in); i++ {
		p := in[i].Val
		if !math.IsNaN(p) {
			if math.IsNaN(diff) {
				diff = p
			} else {
				diff -= p
			}
		}
	}
	return diff
}

func StdDev(in []schema.Point) float64 {
	avg := Avg(in)
	if !math.IsNaN(avg) {
		num := float64(0)
		totalDeviationSquared := float64(0)
		for i := 0; i < len(in); i++ {
			p := in[i].Val
			if !math.IsNaN(p) {
				num++
				deviation := p - avg
				totalDeviationSquared += deviation * deviation
			}
		}
		std := math.Sqrt(totalDeviationSquared / num)
		return std
	}
	return math.NaN()
}

func Range(in []schema.Point) float64 {
	min := Min(in)
	max := Max(in)
	return max - min
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
