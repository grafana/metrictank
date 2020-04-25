package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestOffsetNoInput(t *testing.T) {
	testOffset("no_input", 0, []models.Series{}, []models.Series{}, t)
}

func TestOffsetSingle(t *testing.T) {
	out := []schema.Point{
		{Val: 100, Ts: 10},
		{Val: 100, Ts: 20},
		{Val: 105.5, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 1234567990, Ts: 60},
	}

	testOffset(
		"single",
		100,
		[]models.Series{
			getQuerySeries("a", a),
		},
		[]models.Series{
			getQuerySeries("offset(a,100)", out),
		},
		t,
	)
}

func TestOffsetMultiple(t *testing.T) {
	out := []schema.Point{
		{Val: 100.11, Ts: 10},
		{Val: 100.11, Ts: 20},
		{Val: 105.61, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 1234567990.11, Ts: 60},
	}

	testOffset(
		"single",
		100.11,
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("a2", a),
		},
		[]models.Series{
			getQuerySeries("offset(a,100.11)", out),
			getQuerySeries("offset(a2,100.11)", out),
		},
		t,
	)
}

func testOffset(name string, factor float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewOffset()
	f.(*FuncOffset).in = NewMock(in)
	f.(*FuncOffset).factor = factor

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

func BenchmarkOffset10k_1NoNulls(b *testing.B) {
	benchmarkOffset(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkOffset10k_10NoNulls(b *testing.B) {
	benchmarkOffset(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkOffset10k_100NoNulls(b *testing.B) {
	benchmarkOffset(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkOffset10k_1000NoNulls(b *testing.B) {
	benchmarkOffset(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkOffset10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffset(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffset10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffset(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffset10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffset(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffset10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffset(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkOffset10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffset(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffset10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffset(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffset10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffset(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffset10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffset(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkOffset(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewOffset()
		f.(*FuncOffset).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
