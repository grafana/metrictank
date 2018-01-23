package graphite

import "math"

type Comparator func(p float64) bool

func Eq(good float64) Comparator {
	return func(p float64) bool {
		return (math.IsNaN(good) && math.IsNaN(p)) || p == good
	}
}

func Ge(good float64) Comparator {
	return func(p float64) bool {
		return (math.IsNaN(good) && math.IsNaN(p)) || p >= good
	}
}
