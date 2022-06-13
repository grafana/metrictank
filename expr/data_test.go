package expr

import (
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

var a = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 5.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 50},
	{Val: 1234567890, Ts: 60},
}

var avgZeroa = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 5.5, Ts: 30},
	{Val: 0, Ts: 40},
	{Val: 0, Ts: 50},
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

var allZeros = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 0, Ts: 30},
	{Val: 0, Ts: 40},
	{Val: 0, Ts: 50},
	{Val: 0, Ts: 60},
}

var halfZeros = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 0, Ts: 30},
	{Val: 1, Ts: 40},
	{Val: 2, Ts: 50},
	{Val: 3, Ts: 60},
}

var noZeros = []schema.Point{
	{Val: 1, Ts: 10},
	{Val: 2, Ts: 20},
	{Val: 3, Ts: 30},
	{Val: 4, Ts: 40},
	{Val: 5, Ts: 50},
	{Val: 6, Ts: 60},
}

var allNulls = []schema.Point{
	{Val: math.NaN(), Ts: 10},
	{Val: math.NaN(), Ts: 20},
	{Val: math.NaN(), Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 50},
	{Val: math.NaN(), Ts: 60},
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

var avgZeroab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64 / 2, Ts: 20},
	{Val: (math.MaxFloat64 - 14.5) / 2, Ts: 30},
	{Val: 0, Ts: 40},
	{Val: 617283945, Ts: 50},
	{Val: 617283945, Ts: 60},
}

var avgabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64 / 3, Ts: 20},
	{Val: (math.MaxFloat64 - 13.5) / 3, Ts: 30},
	{Val: 2, Ts: 40},
	{Val: float64(1234567893) / 2, Ts: 50},
	{Val: float64(1234567894) / 2, Ts: 60},
}

var avgZeroabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64 / 3, Ts: 20},
	{Val: (math.MaxFloat64 - 13.5) / 3, Ts: 30},
	{Val: float64(2) / 3, Ts: 40},
	{Val: float64(1234567893) / 3, Ts: 50},
	{Val: float64(1234567894) / 3, Ts: 60},
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

var multab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: math.Inf(1), Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 50}, // in accordance with graphite, mult(5,null) = 5
	{Val: math.NaN(), Ts: 60},
}

var multabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: math.Inf(1), Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 50},
	{Val: math.NaN(), Ts: 60},
}

var medianab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64 / 2, Ts: 20},
	{Val: (math.MaxFloat64 - 14.5) / 2, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 1234567890, Ts: 50},
	{Val: 1234567890, Ts: 60},
}

var medianabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 5.5, Ts: 30},
	{Val: 2, Ts: 40},
	{Val: float64(1234567893) / 2, Ts: 50},
	{Val: float64(1234567894) / 2, Ts: 60},
}

var diffab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: -math.MaxFloat64, Ts: 20},
	{Val: 25.5 - math.MaxFloat64, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 1234567890, Ts: 50},
	{Val: 1234567890, Ts: 60},
}

var diffabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: -math.MaxFloat64, Ts: 20},
	{Val: 24.5 - math.MaxFloat64, Ts: 30},
	{Val: 2, Ts: 40},
	{Val: 1234567887, Ts: 50},
	{Val: 1234567886, Ts: 60},
}

var stddevab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.Inf(1), Ts: 20},
	{Val: math.Inf(1), Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 0, Ts: 50},
	{Val: 0, Ts: 60},
}

var stddevabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.Inf(1), Ts: 20},
	{Val: math.Inf(1), Ts: 30},
	{Val: 0, Ts: 40},
	{Val: 617283943.5, Ts: 50},
	{Val: 617283943, Ts: 60},
}

var rangeab = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64, Ts: 20},
	{Val: math.MaxFloat64 - 14.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 0, Ts: 50},
	{Val: 0, Ts: 60},
}

var rangeabc = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.MaxFloat64, Ts: 20},
	{Val: math.MaxFloat64 - 19, Ts: 30},
	{Val: 0, Ts: 40},
	{Val: 1234567887, Ts: 50},
	{Val: 1234567886, Ts: 60},
}

var counta = []schema.Point{
	{Val: 1, Ts: 10},
	{Val: 1, Ts: 20},
	{Val: 1, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 50},
	{Val: 1, Ts: 60},
}

var countab = []schema.Point{
	{Val: 2, Ts: 10},
	{Val: 2, Ts: 20},
	{Val: 2, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 1, Ts: 50},
	{Val: 1, Ts: 60},
}

var countabc = []schema.Point{
	{Val: 3, Ts: 10},
	{Val: 3, Ts: 20},
	{Val: 3, Ts: 30},
	{Val: 1, Ts: 40},
	{Val: 2, Ts: 50},
	{Val: 2, Ts: 60},
}

// make sure we test with the correct data, don't mask if processing accidentally modifies our input data
func getCopy(in []schema.Point) []schema.Point {
	out := make([]schema.Point, len(in))
	copy(out, in)
	return out
}

func getSeries(target, patt string, data []schema.Point) models.Series {
	from := uint32(10)
	to := uint32(61)
	if len(data) > 0 {
		from = data[0].Ts
		to = data[len(data)-1].Ts + 1
	}
	return models.Series{
		Target:     target,
		QueryPatt:  patt,
		QueryFrom:  from,
		QueryTo:    to,
		Datapoints: getCopy(data),
		Interval:   10,
	}
}

func getSeriesNamed(name string, data []schema.Point) models.Series {
	from := uint32(10)
	to := uint32(61)
	if len(data) > 0 {
		from = data[0].Ts
		to = data[len(data)-1].Ts + 1
	}
	return models.Series{
		Target:     name,
		QueryPatt:  name,
		QueryFrom:  from,
		QueryTo:    to,
		Datapoints: getCopy(data),
		Interval:   10,
	}
}

func getTimeRangeSeriesListNamed(target, patt string, interval, from, to uint32, data ...[]schema.Point) []models.Series {
	outputs := make([]models.Series, 0, len(data))
	for _, datum := range data {
		serie := models.Series{
			Target:     target,
			Interval:   interval,
			QueryPatt:  patt,
			QueryFrom:  from,
			QueryTo:    to,
			Datapoints: getCopy(datum),
		}
		outputs = append(outputs, serie)
	}
	return outputs
}

func getModel(name string, data []schema.Point) models.Series {
	from := uint32(10)
	to := uint32(61)
	if len(data) > 0 {
		from = data[0].Ts
		to = data[len(data)-1].Ts + 1
	}
	series := models.Series{
		Target:     name,
		QueryPatt:  name,
		QueryFrom:  from,
		QueryTo:    to,
		Datapoints: getCopy(data),
		Interval:   10,
	}
	series.SetTags()
	return series
}
