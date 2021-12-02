package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestCountSeriesZero(t *testing.T) {
	testCountSeries("zero", [][]models.Series{}, []models.Series{}, t)
}

func TestCountSeriesFive(t *testing.T) {
	out := []schema.Point{
		{Val: 5, Ts: 10},
		{Val: 5, Ts: 20},
		{Val: 5, Ts: 30},
		{Val: 5, Ts: 40},
		{Val: 5, Ts: 50},
		{Val: 5, Ts: 60},
	}
	testCountSeries(
		"five",
		[][]models.Series{
			{
				getSeriesNamed("abc", a),
				getSeriesNamed("abc", b),
				getSeriesNamed("abc", c),
			},
			{
				getSeriesNamed("ad", d),
				getSeriesNamed("ad", a),
			},
		},
		[]models.Series{
			getSeriesNamed("countSeries(abc,ad)", out),
		},
		t,
	)
}

func testCountSeries(name string, in [][]models.Series, out []models.Series, t *testing.T) {
	f := NewCountSeries()
	for _, i := range in {
		f.(*FuncCountSeries).in = append(f.(*FuncCountSeries).in, NewMock(i))
	}

	inputCopy := make([][]models.Series, len(in)) // to later verify that it is unchanged
	for i := range in {
		inputCopy[i] = models.SeriesCopy(in[i])
	}

	dataMap := initDataMapMultiple(in)
	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		for i := range inputCopy {
			if err := equalOutput(inputCopy[i], in[i], nil, nil); err != nil {
				t.Fatalf("Case %s: Input was modified, err = %s", name, err)
			}
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

func BenchmarkCountSeries10k_1NoNulls(b *testing.B) {
	benchmarkCountSeries(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkCountSeries10k_10NoNulls(b *testing.B) {
	benchmarkCountSeries(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkCountSeries10k_100NoNulls(b *testing.B) {
	benchmarkCountSeries(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkCountSeries10k_1000NoNulls(b *testing.B) {
	benchmarkCountSeries(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkCountSeries10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkCountSeries10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkCountSeries(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
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
		f := NewCountSeries()
		f.(*FuncCountSeries).in = append(f.(*FuncCountSeries).in, NewMock(input))
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
