package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func getNewAbsolute(in []models.Series) *FuncAbsolute {
	f := NewAbsolute()
	ps := f.(*FuncAbsolute)
	ps.in = NewMock(in)
	return ps
}

var random = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: -10, Ts: 20},
	{Val: 5.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: -math.MaxFloat64, Ts: 50},
	{Val: -1234567890, Ts: 60},
	{Val: math.MaxFloat64, Ts: 70},
}

var randomAbsolute = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 10, Ts: 20},
	{Val: 5.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.MaxFloat64, Ts: 50},
	{Val: 1234567890, Ts: 60},
	{Val: math.MaxFloat64, Ts: 70},
}

func TestAbsoluteRandom(t *testing.T) {
	f := getNewAbsolute(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "random",
				Target:     "rand",
				Datapoints: getCopy(random),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "absolute(random)",
			Target:     "absolute(rand)",
			Datapoints: getCopy(randomAbsolute),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}
func BenchmarkAbsolute10k_1NoNulls(b *testing.B) {
	benchmarkAbsolute(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAbsolute10k_10NoNulls(b *testing.B) {
	benchmarkAbsolute(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAbsolute10k_100NoNulls(b *testing.B) {
	benchmarkAbsolute(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAbsolute10k_1000NoNulls(b *testing.B) {
	benchmarkAbsolute(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAbsolute10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAbsolute(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAbsolute10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAbsolute(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAbsolute10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAbsolute(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAbsolute10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAbsolute(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAbsolute10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkAbsolute(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAbsolute10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkAbsolute(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAbsolute10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkAbsolute(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAbsolute10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkAbsolute(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkAbsolute(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewAbsolute()
		f.(*FuncAbsolute).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
