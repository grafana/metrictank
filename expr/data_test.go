package expr

import (
	"math"

	"gopkg.in/raintank/schema.v1"
)

var a = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 5.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 50},
	{Val: 1234567890, Ts: 60},
}

var b = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64, Ts: 20},
	{Val: math.MaxFloat64 - 20, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 1234567890, Ts: 50},
	{Val: math.NaN(), Ts: 60},
}

var c = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 1, Ts: 30},
	{Val: 2, Ts: 40},
	{Val: 3, Ts: 50},
	{Val: 4, Ts: 60},
}

var sumab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64, Ts: 20},
	{Val: math.MaxFloat64 - 14.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 1234567890, Ts: 50}, // graphite says 5+null=5 not null
	{Val: 1234567890, Ts: 60},
}

var sumabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64, Ts: 20},
	{Val: math.MaxFloat64 - 13.5, Ts: 30},
	{Val: 2, Ts: 40},
	{Val: 1234567893, Ts: 50},
	{Val: 1234567894, Ts: 60},
}

var avgab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64 / 2, Ts: 20},
	{Val: (math.MaxFloat64 - 14.5) / 2, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 1234567890, Ts: 50}, // in accordance with graphite, avg(5,null) = 5
	{Val: 1234567890, Ts: 60},
}

var avgabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64 / 3, Ts: 20},
	{Val: (math.MaxFloat64 - 13.5) / 3, Ts: 30},
	{Val: 2, Ts: 40},
	{Val: float64(1234567893) / 2, Ts: 50},
	{Val: float64(1234567894) / 2, Ts: 60},
}

// make sure we test with the correct data, don't mask if processing accidentally modifies our input data
func getCopy(in []schema.Point) []schema.Point {
	out := make([]schema.Point, len(in))
	copy(out, in)
	return out
}
