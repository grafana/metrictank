package graphite

import (
	"fmt"
	"math"
)

type Comparator struct {
	Name string
	Fn   func(p float64) bool
}

// Eq returns a comparator that checks whether a float equals the given float
func Eq(good float64) Comparator {
	return Comparator{
		Name: fmt.Sprintf("Eq(%f)", good),
		Fn: func(p float64) bool {
			return (math.IsNaN(good) && math.IsNaN(p)) || p == good
		},
	}
}

// Eq returns a comparator that checks whether a float is bigger or equal than the given float (or both are NaN)
func Ge(good float64) Comparator {
	return Comparator{
		Name: fmt.Sprintf("Ge(%f)", good),
		Fn: func(p float64) bool {
			return (math.IsNaN(good) && math.IsNaN(p)) || p >= good
		},
	}
}
