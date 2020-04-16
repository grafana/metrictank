package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

var datapoints = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.NaN(), Ts: 20},
	{Val: -10, Ts: 30},
	{Val: 5.5, Ts: 40},
	{Val: -math.MaxFloat64, Ts: 50},
	{Val: -1234567890, Ts: 60},
	{Val: math.MaxFloat64, Ts: 70},
}

var datapointsInvert = []schema.Point{
	{Val: math.NaN(), Ts: 10},
	{Val: math.NaN(), Ts: 20},
	{Val: math.Pow(-10, -1), Ts: 30},
	{Val: math.Pow(5.5, -1), Ts: 40},
	{Val: math.Pow(-math.MaxFloat64, -1), Ts: 50},
	{Val: math.Pow(-1234567890, -1), Ts: 60},
	{Val: math.Pow(math.MaxFloat64, -1), Ts: 70},
}

func TestInvert(t *testing.T) {
	f := getNewInvert(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "queryPattHere",
				Target:     "targetHere",
				Datapoints: getCopy(datapoints),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "invert(queryPattHere)",
			Target:     "invert(targetHere)",
			Datapoints: getCopy(datapointsInvert),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func getNewInvert(in []models.Series) *FuncInvert {
	f := NewInvert()
	ps := f.(*FuncInvert)
	ps.in = NewMock(in)
	return ps
}

func BenchmarkInvert10k_1NoNulls(b *testing.B) {
	benchmarkInvert(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkInvert10k_10NoNulls(b *testing.B) {
	benchmarkInvert(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkInvert10k_100NoNulls(b *testing.B) {
	benchmarkInvert(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkInvert10k_1000NoNulls(b *testing.B) {
	benchmarkInvert(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkInvert10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkInvert(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
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
	for i := 0; i < b.N; i++ {
		f := NewInvert()
		f.(*FuncInvert).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
