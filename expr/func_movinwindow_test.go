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

var resSinglePtInA = []schema.Point{
	{Val: 1, Ts: 20},
	{Val: 2, Ts: 30},
	{Val: 3, Ts: 40},
	{Val: 4, Ts: 50},
	{Val: 5, Ts: 60},
	{Val: 6, Ts: 70},
	{Val: 7, Ts: 80},
}

var avg2PtsA = []schema.Point{
	{Val: 1.5, Ts: 30},
	{Val: 2.5, Ts: 40},
	{Val: 3.5, Ts: 50},
	{Val: 4.5, Ts: 60},
	{Val: 5.5, Ts: 70},
	{Val: 6.5, Ts: 80},
}

var allNullPts = []schema.Point{
	{Val: math.NaN(), Ts: 10},
	{Val: math.NaN(), Ts: 20},
	{Val: math.NaN(), Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 50},
	{Val: math.NaN(), Ts: 60},
	{Val: math.NaN(), Ts: 70},
	{Val: math.NaN(), Ts: 80},
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

var res2PtsB = []schema.Point{
	{Val: 0, Ts: 30},
	{Val: 3, Ts: 40},
	{Val: 3, Ts: 50},
	{Val: 6, Ts: 60},
	{Val: 6, Ts: 70},
	{Val: 9, Ts: 80},
}

var resSinglePtInB = []schema.Point{
	{Val: 0, Ts: 20},
	{Val: math.NaN(), Ts: 30},
	{Val: 3, Ts: 40},
	{Val: math.NaN(), Ts: 50},
	{Val: 6, Ts: 60},
	{Val: math.NaN(), Ts: 70},
	{Val: 9, Ts: 80},
}

var allNullsB = []schema.Point{
	{Val: math.NaN(), Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 50},
	{Val: math.NaN(), Ts: 60},
	{Val: math.NaN(), Ts: 70},
	{Val: math.NaN(), Ts: 80},
}

var seriesC30secs = []schema.Point{
	{Val: 2, Ts: 30},
	{Val: 3, Ts: 60},
	{Val: 4, Ts: 90},
	{Val: 5, Ts: 120},
	{Val: 6, Ts: 150},
	{Val: 7, Ts: 180},
	{Val: 8, Ts: 210},
}

var sumC2Pts = []schema.Point{
	{Val: 2, Ts: 60},
	{Val: 5, Ts: 90},
	{Val: 7, Ts: 120},
	{Val: 9, Ts: 150},
	{Val: 11, Ts: 180},
	{Val: 13, Ts: 210},
}

var minC2Pts = []schema.Point{
	{Val: 2, Ts: 60},
	{Val: 2, Ts: 90},
	{Val: 3, Ts: 120},
	{Val: 4, Ts: 150},
	{Val: 5, Ts: 180},
	{Val: 6, Ts: 210},
}

func getNamedSeries(target, patt string, from, to uint32, data ...[]schema.Point) []models.Series {

	if len(data) == 0 {
		panic("Needs at least one array of schema.Points")
	}

	outputs := make([]models.Series, 0, len(data))
	for _, datum := range data {
		serie := models.Series{
			Target:     target,
			QueryPatt:  patt,
			QueryFrom:  from,
			QueryTo:    to,
			Datapoints: getCopy(datum),
		}
		outputs = append(outputs, serie)
	}
	return outputs
}

func TestMovingWindowWithDefaultValues(t *testing.T) {
	offset10s := uint32(10)
	testMovingWindow(
		"defaults",
		getNamedSeries("t", "p", 10, 80, seriesA, seriesB),
		getNamedSeries("movingAverage(t,\"10s\")", "movingAverage(p,\"10s\")",
			10+offset10s, 80, resSinglePtInA, resSinglePtInB),
		offset10s,
		"10s",
		"",         // defaults to "average"
		math.NaN(), // defaults to 0
		t)

	offset20s := uint32(20)
	testMovingWindow(
		"defaults",
		getNamedSeries("t", "p", 10, 80, seriesA, seriesB),
		getNamedSeries("movingAverage(t,\"20s\")", "movingAverage(p,\"20s\")",
			10+offset20s, 80, avg2PtsA, res2PtsB),
		offset20s,
		"20s",
		"",         // defaults to "average"
		math.NaN(), // defaults to 0
		t)
}

func TestMovingWindowByWindowSizes(t *testing.T) {
	offset10s := uint32(10)
	testMovingWindow(
		"signed window(negative)",
		getNamedSeries("t", "p", 10, 80, seriesA, seriesA),
		getNamedSeries("movingAverage(t,\"-10s\")", "movingAverage(p,\"-10s\")",
			10+offset10s, 80, resSinglePtInA, resSinglePtInA),
		offset10s,
		"-10s",
		"average",
		1,
		t)

	testMovingWindow(
		"signed window (positive)",
		getNamedSeries("t", "p", 10, 80, seriesA, seriesA),
		getNamedSeries("movingSum(t,\"+10s\")", "movingSum(p,\"+10s\")",
			10+offset10s, 80, resSinglePtInA, resSinglePtInA),
		offset10s,
		"+10s",
		"sum",
		1,
		t)

	offsetZero := uint32(0)
	testMovingWindow(
		"empty window",
		getNamedSeries("t", "p", 10, 80, seriesA, seriesB),
		getNamedSeries("movingAverage(t,\"0m\")", "movingAverage(p,\"0m\")",
			10+offsetZero, 80, allNullPts, allNullPts),
		offsetZero,
		"0m",
		"average",
		1,
		t)
}

func TestMovingWindowWithXFilesFactorFilter(t *testing.T) {
	offset20s := uint32(20)
	testMovingWindow(
		"xFilesFactor > 0.5",
		getNamedSeries("t", "p", 10, 80, seriesB),
		getNamedSeries("movingSum(t,\"20s\")", "movingSum(p,\"20s\")",
			10+offset20s, 80, allNullsB),
		offset20s,
		"20s",
		"sum",
		0.66,
		t)

	testMovingWindow(
		"xFilesFactor < 0.5",
		getNamedSeries("t", "p", 10, 80, seriesB),
		getNamedSeries("movingSum(t,\"20s\")", "movingSum(p,\"20s\")",
			10+offset20s, 80, res2PtsB),
		offset20s,
		"20s",
		"sum",
		0.33,
		t)

	testMovingWindow(
		"xFilesFactor 1",
		getNamedSeries("t", "p", 10, 80, seriesB),
		getNamedSeries("movingSum(t,\"20s\")", "movingSum(p,\"20s\")",
			10+offset20s, 80, allNullsB),
		offset20s,
		"20s",
		"sum",
		1,
		t)

	testMovingWindow(
		"xFilesFactor 0",
		getNamedSeries("t", "p", 10, 80, seriesB),
		getNamedSeries("movingSum(t,\"20s\")", "movingSum(p,\"20s\")",
			10+offset20s, 80, res2PtsB),
		offset20s,
		"20s",
		"sum",
		0,
		t)
}

func TestMovingWindowWindowWhenTimeShiftGoesBeyondAvailableSeriesStartPoints(t *testing.T) {
	offset1min := uint32(60)

	testMovingWindow(
		"movingSum of 1 min windowSize",
		getNamedSeries("t", "p", 0, 210, seriesC30secs),
		getNamedSeries("movingSum(t,\"1min\")", "movingSum(p,\"1min\")",
			0+offset1min, 210, sumC2Pts),
		offset1min,
		"1min",
		"sum",
		0,
		t)

	testMovingWindow(
		"movingMin of 1 min windowSize",
		getNamedSeries("t", "p", 0, 210, seriesC30secs),
		getNamedSeries("movingMin(t,\"1min\")", "movingMin(p,\"1min\")",
			0+offset1min, 210, minC2Pts),
		offset1min,
		"1min",
		"min",
		0,
		t)
}

func testMovingWindow(name string, in []models.Series, out []models.Series, offset uint32, windowSize, fn string, xFilesFactor float64, t *testing.T) {
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

	// Calling Context causes the time shift in QueryFrom
	callContext(name, f, in[0].QueryFrom, in[0].QueryTo, offset, t)

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal("Failed test:", name, err)
	}

}

func callContext(name string, f GraphiteFunc, from, to, offset uint32, t *testing.T) {
	context := Context{
		from: from,
		to:   to,
	}
	newContext := f.Context(context)
	if newContext.from != from-offset {
		t.Fatalf("case %q: Expected context.from = %d, got %d", name, (from - offset), newContext.from)
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
