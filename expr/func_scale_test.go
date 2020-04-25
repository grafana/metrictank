package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestScaleNoInput(t *testing.T) {
	testScale("no_input", 0, []models.Series{}, []models.Series{}, t)
}

func TestScaleSingle(t *testing.T) {
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 550, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 123456789000, Ts: 60},
	}

	testScale(
		"single",
		100,
		[]models.Series{
			getQuerySeries("a", a),
		},
		[]models.Series{
			getQuerySeries("scale(a,100)", out),
		},
		t,
	)
}

func TestScaleMultiple(t *testing.T) {
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 550.605, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 123592591467.9, Ts: 60},
	}

	testScale(
		"single",
		100.11,
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("a2", a),
		},
		[]models.Series{
			getQuerySeries("scale(a,100.11)", out),
			getQuerySeries("scale(a2,100.11)", out),
		},
		t,
	)
}

func testScale(name string, factor float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewScale()
	f.(*FuncScale).in = NewMock(in)
	f.(*FuncScale).factor = factor

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

func BenchmarkScale10k_1NoNulls(b *testing.B) {
	benchmarkScale(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkScale10k_10NoNulls(b *testing.B) {
	benchmarkScale(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkScale10k_100NoNulls(b *testing.B) {
	benchmarkScale(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkScale10k_1000NoNulls(b *testing.B) {
	benchmarkScale(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkScale10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkScale(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScale10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkScale(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScale10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkScale(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScale10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkScale(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkScale10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkScale(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScale10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkScale(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScale10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkScale(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScale10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkScale(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkScale(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewScale()
		f.(*FuncScale).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
