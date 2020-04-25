package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestKeepLastValueAll(t *testing.T) {
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5, Ts: 30},
		{Val: 5.5, Ts: 40},
		{Val: 5.5, Ts: 50},
		{Val: 1234567890, Ts: 60},
	}

	testKeepLastValue(
		"keepAll",
		math.MaxInt64,
		[]models.Series{
			getQuerySeries("a", a),
		},
		[]models.Series{
			getQuerySeries("keepLastValue(a)", out),
		},
		t,
	)
}

func TestKeepLastValueNone(t *testing.T) {

	testKeepLastValue(
		"keepNone",
		0,
		[]models.Series{
			getQuerySeries("sum4a2b", sum4a2b),
		},
		[]models.Series{
			getQuerySeries("keepLastValue(sum4a2b)", sum4a2b),
		},
		t,
	)
}

func TestKeepLastValueOne(t *testing.T) {
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: math.MaxFloat64, Ts: 20},
		{Val: math.MaxFloat64 - 20, Ts: 30},
		{Val: math.MaxFloat64 - 20, Ts: 40},
		{Val: 1234567890, Ts: 50},
		{Val: 1234567890, Ts: 60},
	}

	testKeepLastValue(
		"keepOne",
		1,
		[]models.Series{
			getQuerySeries("b", b),
			getQuerySeries("a", a),
		},
		[]models.Series{
			getQuerySeries("keepLastValue(b)", out),
			getQuerySeries("keepLastValue(a)", a),
		},
		t,
	)
}

func testKeepLastValue(name string, limit int64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewKeepLastValue()
	f.(*FuncKeepLastValue).in = NewMock(in)
	f.(*FuncKeepLastValue).limit = limit

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

func BenchmarkKeepLastValue10k_1NoNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkKeepLastValue10k_10NoNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkKeepLastValue10k_100NoNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkKeepLastValue10k_1000NoNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkKeepLastValue10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkKeepLastValue10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkKeepLastValue(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewKeepLastValue()
		f.(*FuncKeepLastValue).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
