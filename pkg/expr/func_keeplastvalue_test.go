package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/schema"
	"github.com/grafana/metrictank/pkg/test"
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
			getSeriesNamed("a", a),
		},
		[]models.Series{
			getSeriesNamed("keepLastValue(a)", out),
		},
		t,
	)
}

func TestKeepLastValueNone(t *testing.T) {

	testKeepLastValue(
		"keepNone",
		0,
		[]models.Series{
			getSeriesNamed("sum4a2b", sum4a2b),
		},
		[]models.Series{
			getSeriesNamed("keepLastValue(sum4a2b)", sum4a2b),
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
			getSeriesNamed("b", b),
			getSeriesNamed("a", a),
		},
		[]models.Series{
			getSeriesNamed("keepLastValue(b)", out),
			getSeriesNamed("keepLastValue(a)", a),
		},
		t,
	)
}

func testKeepLastValue(name string, limit int64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewKeepLastValue()
	f.(*FuncKeepLastValue).in = NewMock(in)
	f.(*FuncKeepLastValue).limit = limit

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

func benchmarkKeepLastValue(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
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
		f := NewKeepLastValue()
		f.(*FuncKeepLastValue).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
