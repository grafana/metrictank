package expr

import (
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/internal/test"
	"github.com/grafana/metrictank/pkg/api/models"
)

func TestRemoveAbovePercentileSingleAllNonNull(t *testing.T) {
	testRemoveAboveBelowPercentile(
		"removeAbovePercentile",
		true,
		60,
		[]models.Series{
			getSeries("a", "abcd", a),
			getSeries("b", "abcd", b),
			getSeries("c", "abcd", c),
			getSeries("d", "abcd", d),
		},
		[]models.Series{
			getSeriesNamed("removeAbovePercentile(a, 60)", []schema.Point{
				{Val: 0, Ts: 10},
				{Val: 0, Ts: 20},
				{Val: 5.5, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: math.NaN(), Ts: 60}}),
			getSeriesNamed("removeAbovePercentile(b, 60)", []schema.Point{
				{Val: 0, Ts: 10},
				{Val: math.MaxFloat64, Ts: 20},
				{Val: math.MaxFloat64 - 20, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: 1234567890, Ts: 50},
				{Val: math.NaN(), Ts: 60}}),
			getSeriesNamed("removeAbovePercentile(c, 60)", []schema.Point{
				{Val: 0, Ts: 10},
				{Val: 0, Ts: 20},
				{Val: 1, Ts: 30},
				{Val: 2, Ts: 40},
				{Val: 3, Ts: 50},
				{Val: math.NaN(), Ts: 60}}),
			getSeriesNamed("removeAbovePercentile(d, 60)", []schema.Point{
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

func TestRemoveBelowPercentileSingleAllNonNull(t *testing.T) {
	testRemoveAboveBelowPercentile(
		"removeBelowPercentile",
		false,
		50,
		[]models.Series{
			getSeries("a", "abcd", a),
			getSeries("b", "abcd", b),
			getSeries("c", "abcd", c),
			getSeries("d", "abcd", d),
		},
		[]models.Series{
			getSeriesNamed("removeBelowPercentile(a, 50)", []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: 5.5, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: 1234567890, Ts: 60}}),
			getSeriesNamed("removeBelowPercentile(b, 50)", []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.MaxFloat64, Ts: 20},
				{Val: math.MaxFloat64 - 20, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: math.NaN(), Ts: 60}}),
			getSeriesNamed("removeBelowPercentile(c, 50)", []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: math.NaN(), Ts: 30},
				{Val: 2, Ts: 40},
				{Val: 3, Ts: 50},
				{Val: 4, Ts: 60}}),
			getSeriesNamed("removeBelowPercentile(d, 50)", []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: 199, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: 80, Ts: 50},
				{Val: 250, Ts: 60}}),
		},
		t,
	)
}

// TestRemoveBelowPercentileWithNulls verifies that, just like Graphite, series with all nulls, are removed from the output
func TestRemoveBelowPercentileWithNulls(t *testing.T) {
	testRemoveAboveBelowPercentile(
		"removeBelowPercentile",
		false,
		50,
		[]models.Series{
			getSeries("a", "abcd", a),
			getSeries("b", "abcd", allNulls),
		},
		[]models.Series{
			getSeriesNamed("removeBelowPercentile(a, 50)", []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: 5.5, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: 1234567890, Ts: 60}}),
		},
		t,
	)
	testRemoveAboveBelowPercentile(
		"removeBelowPercentile",
		false,
		50,
		[]models.Series{
			getSeries("b", "abcd", allNulls),
		},
		[]models.Series{},
		t,
	)
}

func testRemoveAboveBelowPercentile(name string, above bool, n float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewRemoveAboveBelowPercentileConstructor(above)()
	f.(*FuncRemoveAboveBelowPercentile).in = NewMock(in)
	f.(*FuncRemoveAboveBelowPercentile).n = n

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
func BenchmarkRemoveAboveBelowPercentile10k_1NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_10NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_100NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_1000NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowPercentile10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowPercentile(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkRemoveAboveBelowPercentile(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
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
		f := NewRemoveAboveBelowPercentileConstructor(rand.Int()%2 == 0)()
		f.(*FuncRemoveAboveBelowPercentile).in = NewMock(input)
		f.(*FuncRemoveAboveBelowPercentile).n = float64(rand.Int()%100 + 1)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
