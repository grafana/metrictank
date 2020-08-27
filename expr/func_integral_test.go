package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

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
	testIntegral(
		[]models.Series{
			getSeries("c", "c", c),
		},
		[]models.Series{
			getSeries("integral(c)", "integral(c)", cIntegral),
		},
		t)
}

func TestIntegralWithNulls(t *testing.T) {
	testIntegral(
		[]models.Series{
			getSeries("a", "a", a),
		},
		[]models.Series{
			getSeries("integral(a)", "integral(a)", aIntegral),
		},
		t)
}

func getNewIntegral(in []models.Series) *FuncIntegral {
	f := NewIntegral()
	ps := f.(*FuncIntegral)
	ps.in = NewMock(in)
	return ps
}

func testIntegral(in []models.Series, out []models.Series, t *testing.T) {
	f := getNewIntegral(in)

	inputCopy := models.SeriesCopy(in) // to later verify that it is unchanged

	dataMap := initDataMap(in)

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Input was modified, err = %s", err)
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Point slices in datamap overlap, err = %s", err)
		}
	})
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
