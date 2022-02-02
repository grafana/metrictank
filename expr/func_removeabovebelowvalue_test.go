package expr

import (
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestRemoveAboveValueSingleAllNonNull(t *testing.T) {
	testRemoveAboveBelowValue(
		"removeAboveValue",
		true,
		199,
		[]models.Series{
			getSeries("a", "abcd", a),
			getSeries("b", "abcd", b),
			getSeries("c", "abcd", c),
			getSeries("d", "abcd", d),
		},
		[]models.Series{
			getSeriesNamed("removeAboveValue(a, 199)", []schema.Point{
				{Val: 0, Ts: 10},
				{Val: 0, Ts: 20},
				{Val: 5.5, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: math.NaN(), Ts: 60}}),
			getSeriesNamed("removeAboveValue(b, 199)", []schema.Point{
				{Val: 0, Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: math.NaN(), Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: math.NaN(), Ts: 60}}),
			getSeriesNamed("removeAboveValue(c, 199)", []schema.Point{
				{Val: 0, Ts: 10},
				{Val: 0, Ts: 20},
				{Val: 1, Ts: 30},
				{Val: 2, Ts: 40},
				{Val: 3, Ts: 50},
				{Val: 4, Ts: 60}}),
			getSeriesNamed("removeAboveValue(d, 199)", []schema.Point{
				{Val: 0, Ts: 10},
				{Val: 33, Ts: 20},
				{Val: 199, Ts: 30},
				{Val: 29, Ts: 40},
				{Val: 80, Ts: 50},
				{Val: math.NaN(), Ts: 60}}),
		},
		t,
	)
}

func TestRemoveBelowValueSingleAllNonNull(t *testing.T) {
	testRemoveAboveBelowValue(
		"removeBelowValue",
		false,
		199,
		[]models.Series{
			getSeries("a", "abcd", a),
			getSeries("b", "abcd", b),
			getSeries("c", "abcd", c),
			getSeries("d", "abcd", d),
		},
		[]models.Series{
			getSeriesNamed("removeBelowValue(a, 199)", []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: math.NaN(), Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: 1234567890, Ts: 60}}),
			getSeriesNamed("removeBelowValue(b, 199)", []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.MaxFloat64, Ts: 20},
				{Val: math.MaxFloat64 - 20, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: 1234567890, Ts: 50},
				{Val: math.NaN(), Ts: 60}}),
			getSeriesNamed("removeBelowValue(c, 199)", []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: math.NaN(), Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: math.NaN(), Ts: 60}}),
			getSeriesNamed("removeBelowValue(d, 199)", []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: 199, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: 250, Ts: 60}}),
		},
		t,
	)
}

func testRemoveAboveBelowValue(name string, above bool, n float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewRemoveAboveBelowValueConstructor(above)()
	f.(*FuncRemoveAboveBelowValue).in = NewMock(in)
	f.(*FuncRemoveAboveBelowValue).n = n

	inputCopy := models.SeriesCopy(in) // to later verify that it is unchanged

	dataMap := initDataMap(in)

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
func BenchmarkRemoveAboveBelowValue10k_1NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowValue10k_10NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowValue10k_100NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowValue10k_1000NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowValue10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkRemoveAboveBelowValue(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			QueryPatt: strconv.Itoa(i),
		}
		if i%2 == 0 {
			series.Datapoints, series.Interval = fn0()
		} else {
			series.Datapoints, series.Interval = fn1()
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewRemoveAboveBelowValueConstructor(rand.Int()%2 == 0)()
		f.(*FuncRemoveAboveBelowValue).in = NewMock(input)
		f.(*FuncRemoveAboveBelowValue).n = rand.Float64()
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
