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

var avg4a2b = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.Inf(0), Ts: 20},
	{Val: math.Inf(0), Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 1234567890, Ts: 50}, // in accordance with graphite, avg(5,null) = 5
	{Val: 1234567890, Ts: 60},
}

var sum4a2b = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.Inf(0), Ts: 20},
	{Val: math.Inf(0), Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 2469135780, Ts: 50}, // in accordance with graphite, sum(5,null) = 5
	{Val: 4938271560, Ts: 60},
}

var c = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 1, Ts: 30},
	{Val: 2, Ts: 40},
	{Val: 3, Ts: 50},
	{Val: 4, Ts: 60},
}

// emulate an 8 bit counter
var d = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 33, Ts: 20},
	{Val: 199, Ts: 30},
	{Val: 29, Ts: 40}, // overflowed
	{Val: 80, Ts: 50},
	{Val: 250, Ts: 60},
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

var sumcd = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 33, Ts: 20},
	{Val: 200, Ts: 30},
	{Val: 31, Ts: 40}, // overflowed
	{Val: 83, Ts: 50},
	{Val: 254, Ts: 60},
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

var maxab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64, Ts: 20},
	{Val: math.MaxFloat64 - 20, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 1234567890, Ts: 50}, // in accordance with graphite, max(5,null) = 5
	{Val: 1234567890, Ts: 60},
}

var maxabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64, Ts: 20},
	{Val: math.MaxFloat64 - 20, Ts: 30},
	{Val: 2, Ts: 40},
	{Val: 1234567890, Ts: 50},
	{Val: 1234567890, Ts: 60},
}

var minab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 5.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 1234567890, Ts: 50}, // in accordance with graphite, min(5,null) = 5
	{Val: 1234567890, Ts: 60},
}

var minabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 1, Ts: 30},
	{Val: 2, Ts: 40},
	{Val: 3, Ts: 50},
	{Val: 4, Ts: 60},
}

// make sure we test with the correct data, don't mask if processing accidentally modifies our input data
func getCopy(in []schema.Point) []schema.Point {
	out := make([]schema.Point, len(in))
	copy(out, in)
	return out
}
