package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestOffsetToZeroNoInput(t *testing.T) {
	testOffsetToZero("no_input", []models.Series{}, []models.Series{}, t)
}

func TestOffsetToZeroSingle(t *testing.T) {
	const offset = 10
	in := []schema.Point{
		{Val: 0 + offset, Ts: 10},
		{Val: 0 + offset, Ts: 20},
		{Val: 5.5 + offset, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 1234567890 + offset, Ts: 60},
	}
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 1234567890, Ts: 60},
	}

	testOffsetToZero(
		"single",
		[]models.Series{
			getSeriesNamed("a", in),
		},
		[]models.Series{
			getSeriesNamed("offsetToZero(a)", out),
		},
		t,
	)
}

func TestOffsetToZeroMultiple(t *testing.T) {
	const offset = 10
	in := []schema.Point{
		{Val: 0 + offset, Ts: 10},
		{Val: 0 + offset, Ts: 20},
		{Val: 5.5 + offset, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 1234567890 + offset, Ts: 60},
	}
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 1234567890, Ts: 60},
	}

	testOffsetToZero(
		"single",
		[]models.Series{
			getSeriesNamed("a", in),
			getSeriesNamed("a2", in),
		},
		[]models.Series{
			getSeriesNamed("offsetToZero(a)", out),
			getSeriesNamed("offsetToZero(a2)", out),
		},
		t,
	)
}

func testOffsetToZero(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := NewOffsetToZero()
	f.(*FuncOffsetToZero).in = NewMock(in)

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

	t.Run("OutputIsCanonical", func(t *testing.T) {
		for i, s := range got {
			if !s.IsCanonical() {
				t.Fatalf("Case %s: output series %d is not canonical: %v", name, i, s)
			}
		}
	})
}

func BenchmarkOffsetToZero10k_1NoNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkOffsetToZero10k_10NoNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkOffsetToZero10k_100NoNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkOffsetToZero10k_1000NoNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkOffsetToZero10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkOffsetToZero10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkOffsetToZero(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewOffsetToZero()
		f.(*FuncOffsetToZero).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
