package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

var seriesA = []schema.Point{
	{Val: 1, Ts: 10},
	{Val: 2, Ts: 20},
	{Val: 3, Ts: 30},
	{Val: 4, Ts: 40},
	{Val: 5, Ts: 50},
	{Val: 6, Ts: 60},
	{Val: 7, Ts: 70},
	{Val: 8, Ts: 80},
}

var avgALast10mins = []schema.Point{
	{Val: 1.5, Ts: 20},
	{Val: 2.5, Ts: 30},
	{Val: 3.5, Ts: 40},
	{Val: 4.5, Ts: 50},
	{Val: 5.5, Ts: 60},
	{Val: 6.5, Ts: 70},
	{Val: 7.5, Ts: 80},
}

var sumALast10mins = []schema.Point{
	{Val: 3, Ts: 20},
	{Val: 5, Ts: 30},
	{Val: 7, Ts: 40},
	{Val: 9, Ts: 50},
	{Val: 11, Ts: 60},
	{Val: 13, Ts: 70},
	{Val: 15, Ts: 80},
}

var avgALast20mins = []schema.Point{
	{Val: 2, Ts: 30},
	{Val: 3, Ts: 40},
	{Val: 4, Ts: 50},
	{Val: 5, Ts: 60},
	{Val: 6, Ts: 70},
	{Val: 7, Ts: 80},
}

var seriesB = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.NaN(), Ts: 20},
	{Val: 3, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 6, Ts: 50},
	{Val: math.NaN(), Ts: 60},
	{Val: 9, Ts: 70},
	{Val: math.NaN(), Ts: 80},
}

var sumBLast20minsWithOneThirdNullsInWindowAllowed = []schema.Point{
	{Val: 3, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 9, Ts: 50},
	{Val: math.NaN(), Ts: 60},
	{Val: 15, Ts: 70},
	{Val: math.NaN(), Ts: 80},
}

var sumBLast20minsWithTwoThirdNullsInWindowAllowed = []schema.Point{
	{Val: 3, Ts: 30},
	{Val: 3, Ts: 40},
	{Val: 9, Ts: 50},
	{Val: 6, Ts: 60},
	{Val: 15, Ts: 70},
	{Val: 9, Ts: 80},
}

func TestMovingWindowWithDefaultValues(t *testing.T) {
	offset10mins := uint32(10)
	testMovingWindow(
		"Multiple Series With Defaults, 10 min window",
		[]models.Series{
			{
				Target:     "first",
				QueryPatt:  "first",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(seriesA),
			},
			{
				Target:     "second",
				QueryPatt:  "second",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(seriesA),
			},
		},
		[]models.Series{
			{
				Target:     "movingAverage(first,\"10min\")",
				QueryPatt:  "movingAverage(first,\"10min\")",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(avgALast10mins),
			},
			{
				Target:     "movingAverage(second,\"10min\")",
				QueryPatt:  "movingAverage(second,\"10min\")",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(avgALast10mins),
			},
		},
		offset10mins,
		"10min",
		"",         // Use default aggregate function
		math.NaN(), // Use default xFilesFactor value
		t)

	offset20mins := uint32(20)
	testMovingWindow(
		"Single Series With Defaults, 20min window",
		[]models.Series{
			{
				Target:     "first",
				QueryPatt:  "first",
				QueryFrom:  10 + offset20mins,
				QueryTo:    80,
				Datapoints: getCopy(seriesA),
			},
		},
		[]models.Series{
			{
				Target:     "movingAverage(first,\"20min\")",
				QueryPatt:  "movingAverage(first,\"20min\")",
				QueryFrom:  10 + offset20mins,
				QueryTo:    80,
				Datapoints: getCopy(avgALast20mins),
			},
		},
		offset20mins,
		"20min",
		"",         // Use default aggregate function
		math.NaN(), // Use default xFilesFactor value
		t)
}

func TestMovingWindowByFunctionName(t *testing.T) {
	offset10mins := uint32(10)
	testMovingWindow(
		"Calling movingAverage",
		[]models.Series{
			{
				Target:     "first",
				QueryPatt:  "first",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(seriesA),
			},
			{
				Target:     "second",
				QueryPatt:  "second",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(seriesA),
			},
		},
		[]models.Series{
			{
				Target:     "movingAverage(first,\"10min\")",
				QueryPatt:  "movingAverage(first,\"10min\")",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(avgALast10mins),
			},
			{
				Target:     "movingAverage(second,\"10min\")",
				QueryPatt:  "movingAverage(second,\"10min\")",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(avgALast10mins),
			},
		},
		offset10mins,
		"10min",
		"average",
		1,
		t)

	testMovingWindow(
		"Calling movingSum",
		[]models.Series{
			{
				Target:     "first",
				QueryPatt:  "first",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(seriesA),
			},
			{
				Target:     "second",
				QueryPatt:  "second",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(seriesA),
			},
		},
		[]models.Series{
			{
				Target:     "movingSum(first,\"10min\")",
				QueryPatt:  "movingSum(first,\"10min\")",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(sumALast10mins),
			},
			{
				Target:     "movingSum(second,\"10min\")",
				QueryPatt:  "movingSum(second,\"10min\")",
				QueryFrom:  10 + offset10mins,
				QueryTo:    80,
				Datapoints: getCopy(sumALast10mins),
			},
		},
		offset10mins,
		"10min",
		"sum",
		1,
		t)
}

func TestMovingWindowWithXFilesFactorFilter(t *testing.T) {
	offset20mins := uint32(20)
	testMovingWindow(
		"movingSum with xFilesFactor 1/3 nulls in window",
		[]models.Series{
			{
				Target:     "first",
				QueryPatt:  "first",
				QueryFrom:  10 + offset20mins,
				QueryTo:    80,
				Datapoints: getCopy(seriesB),
			},
		},
		[]models.Series{
			{
				Target:     "movingSum(first,\"20min\")",
				QueryPatt:  "movingSum(first,\"20min\")",
				QueryFrom:  10 + offset20mins,
				QueryTo:    80,
				Datapoints: getCopy(sumBLast20minsWithOneThirdNullsInWindowAllowed),
			},
		},
		offset20mins,
		"20min",
		"sum",
		0.66, // xFilesFactor 2/3 non null
		t)

	testMovingWindow(
		"movingSum with xFilesFactor 2/3 nulls in window",
		[]models.Series{
			{
				Target:     "first",
				QueryPatt:  "first",
				QueryFrom:  10 + offset20mins,
				QueryTo:    80,
				Datapoints: getCopy(seriesB),
			},
		},
		[]models.Series{
			{
				Target:     "movingSum(first,\"20min\")",
				QueryPatt:  "movingSum(first,\"20min\")",
				QueryFrom:  10 + offset20mins,
				QueryTo:    80,
				Datapoints: getCopy(sumBLast20minsWithTwoThirdNullsInWindowAllowed),
			},
		},
		offset20mins,
		"20min",
		"sum",
		0.33, // xFilesFactor 1/3 non null
		t)
}

func testMovingWindow(name string, in []models.Series, out []models.Series, offset uint32, windowSize, fn string, xFilesFactor float64, t *testing.T) {

	// Below we simulate the effect of the time
	// modifications in the function Context
	// QueryFrom is reset. QueryTo is left untouched.
	for i := range in {
		in[i].QueryFrom = in[i].Datapoints[0].Ts
	}

	var f GraphiteFunc
	if fn != "" {
		f = NewMovingWindowConstructor(fn)()
		f.(*FuncMovingWindow).fn = fn
	} else {
		f = NewMovingWindow()
	}

	f.(*FuncMovingWindow).in = NewMock(in)
	f.(*FuncMovingWindow).windowSize = windowSize
	f.(*FuncMovingWindow).shiftOffset = offset
	if !math.IsNaN(xFilesFactor) {
		f.(*FuncMovingWindow).xFilesFactor = xFilesFactor
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal("Failed test:", name, err)
	}

}

func BenchmarkMovingWindow10k_1NoNulls(b *testing.B) {
	benchmarkMovingWindow(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkMovingWindow10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkMovingWindow(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMovingWindow10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkMovingWindow(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkMovingWindow(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target:    strconv.Itoa(i),
			QueryPatt: strconv.Itoa(i),
		}
		if i%2 == 0 {
			series.Datapoints = fn0()
		} else {
			series.Datapoints = fn1()
		}
		input = append(input, series)
	}
	b.ResetTimer()
	dataMap := DataMap(make(map[Req][]models.Series))
	for i := 0; i < b.N; i++ {
		f := NewMovingWindow()
		f.(*FuncMovingWindow).in = NewMock(input)
		f.(*FuncMovingWindow).windowSize = "10m"
		got, err := f.Exec(dataMap)
		if err != nil {
			b.Fatalf("%s", err)
		}
		dataMap.Clean()
		results = got
	}
}
