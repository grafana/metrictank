package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func getNewIntegral(in []models.Series) *FuncIntegral {
	f := NewIntegral()
	ps := f.(*FuncIntegral)
	ps.in = NewMock(in)
	return ps
}

var aIntegral = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 5.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 50},
	{Val: 1234567895.5, Ts: 60},
}

var cIntegral = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 1, Ts: 30},
	{Val: 3, Ts: 40},
	{Val: 6, Ts: 50},
	{Val: 10, Ts: 60},
}

func TestIntegralNoNulls(t *testing.T) {
	f := getNewIntegral(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "c",
				Target:     "c",
				Datapoints: getCopy(c),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "integral(c)",
			Target:     "integral(c)",
			Datapoints: cIntegral,
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestIntegralWithNulls(t *testing.T) {
	f := getNewIntegral(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a",
				Target:     "a",
				Datapoints: getCopy(a),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "integral(a)",
			Target:     "integral(a)",
			Datapoints: aIntegral,
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}
func BenchmarkIntegral10k_1NoNulls(b *testing.B) {
	benchmarkIntegral(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkIntegral10k_10NoNulls(b *testing.B) {
	benchmarkIntegral(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkIntegral10k_100NoNulls(b *testing.B) {
	benchmarkIntegral(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkIntegral10k_1000NoNulls(b *testing.B) {
	benchmarkIntegral(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkIntegral10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkIntegral(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIntegral10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkIntegral(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIntegral10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkIntegral(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIntegral10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkIntegral(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIntegral10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkIntegral(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIntegral10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkIntegral(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIntegral10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkIntegral(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIntegral10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkIntegral(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkIntegral(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewIntegral()
		f.(*FuncIntegral).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
