package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestNonNegativeDerivativeNoMax(t *testing.T) {
	testNonNegativeDerivative(
		"no-max-value",
		math.NaN(),
		[]models.Series{
			getSeries("a", "abcd", a),
			getSeries("b", "abcd", b),
			getSeries("c", "abcd", c),
			getSeries("d", "abcd", d),
		},
		[]models.Series{
			getSeries(
				"nonNegativeDerivative(a)",
				"nonNegativeDerivative(abcd)",
				[]schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 5.5, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60}}),
			getSeries(
				"nonNegativeDerivative(b)",
				"nonNegativeDerivative(abcd)",
				[]schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: math.MaxFloat64, Ts: 20},
					{Val: 0, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60}}),
			getSeries(
				"nonNegativeDerivative(c)",
				"nonNegativeDerivative(abcd)",
				[]schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 1, Ts: 30},
					{Val: 1, Ts: 40},
					{Val: 1, Ts: 50},
					{Val: 1, Ts: 60}}),
			getSeries(
				"nonNegativeDerivative(d)",
				"nonNegativeDerivative(abcd)",
				[]schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: 33, Ts: 20},
					{Val: 166, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: 51, Ts: 50},
					{Val: 170, Ts: 60}}),
		},
		t,
	)
}

func TestNonNegativeDerivativeWithMax(t *testing.T) {
	testNonNegativeDerivative(
		"with-max-value",
		199,
		[]models.Series{
			getSeries("a", "abcd", a),
			getSeries("b", "abcd", b),
			getSeries("c", "abcd", c),
			getSeries("d", "abcd", d),
		},
		[]models.Series{
			getSeries(
				"nonNegativeDerivative(a)",
				"nonNegativeDerivative(abcd)",
				[]schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 5.5, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60}}),
			getSeries(
				"nonNegativeDerivative(b)",
				"nonNegativeDerivative(abcd)",
				[]schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: math.NaN(), Ts: 20},
					{Val: math.NaN(), Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60}}),
			getSeries(
				"nonNegativeDerivative(c)",
				"nonNegativeDerivative(abcd)",
				[]schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 1, Ts: 30},
					{Val: 1, Ts: 40},
					{Val: 1, Ts: 50},
					{Val: 1, Ts: 60}}),
			getSeries(
				"nonNegativeDerivative(d)",
				"nonNegativeDerivative(abcd)",
				[]schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: 33, Ts: 20},
					{Val: 166, Ts: 30},
					{Val: 30, Ts: 40},
					{Val: 51, Ts: 50},
					{Val: math.NaN(), Ts: 60}}),
		},
		t,
	)
}

func testNonNegativeDerivative(name string, maxValue float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewNonNegativeDerivative()
	f.(*FuncNonNegativeDerivative).in = NewMock(in)
	f.(*FuncNonNegativeDerivative).maxValue = maxValue

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

	dataMap := DataMap(make(map[Req][]models.Series))

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Case %s: Input was modified, err = %s", name, err)
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", name, err)
		}
	})
}
func BenchmarkNonNegativeDerivative10k_1NoNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkNonNegativeDerivative10k_10NoNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkNonNegativeDerivative10k_100NoNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkNonNegativeDerivative10k_1000NoNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkNonNegativeDerivative10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkNonNegativeDerivative10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkNonNegativeDerivative10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkNonNegativeDerivative10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkNonNegativeDerivative10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkNonNegativeDerivative10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkNonNegativeDerivative10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkNonNegativeDerivative10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkNonNegativeDerivative(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkNonNegativeDerivative(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewNonNegativeDerivative()
		f.(*FuncNonNegativeDerivative).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
