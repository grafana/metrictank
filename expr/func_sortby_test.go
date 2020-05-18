package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestSortByAverage(t *testing.T) {
	testSortBy(
		"sortBy(average,false)",
		"average",
		false,
		[]models.Series{
			getSeriesNamed("a", a),
		},
		[]models.Series{
			getSeriesNamed("a", a),
		},
		t,
	)
}

func TestSortByAverageReverse(t *testing.T) {
	testSortBy(
		"sortBy(average,true)",
		"average",
		true,
		[]models.Series{
			getSeriesNamed("c", c),
			getSeriesNamed("b", b),
			getSeriesNamed("a", a),
		},
		[]models.Series{
			getSeriesNamed("b", b),
			getSeriesNamed("a", a),
			getSeriesNamed("c", c),
		},
		t,
	)
}

func TestSortByCurrent(t *testing.T) {
	testSortBy(
		"sortBy(current,false)",
		"current",
		false,
		[]models.Series{
			getSeriesNamed("avg4a2b", avg4a2b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
		},
		[]models.Series{
			getSeriesNamed("avg4a2b", avg4a2b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
			getSeriesNamed("sum4a2b", sum4a2b),
		},
		t,
	)
}

func TestSortByCurrentReverse(t *testing.T) {
	testSortBy(
		"sortBy(current,true)",
		"current",
		true,
		[]models.Series{
			getSeriesNamed("sumab", sumab),
			getSeriesNamed("b", b),
		},
		[]models.Series{
			getSeriesNamed("sumab", sumab),
			getSeriesNamed("b", b),
		},
		t,
	)
}

func TestSortByMax(t *testing.T) {
	testSortBy(
		"sortBy(max,false)",
		"max",
		false,
		[]models.Series{
			getSeriesNamed("avg4a2b", avg4a2b),
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("b", b),
		},
		[]models.Series{
			getSeriesNamed("b", b),
			getSeriesNamed("avg4a2b", avg4a2b),
			getSeriesNamed("sum4a2b", sum4a2b),
		},
		t,
	)
}

func TestSortByMaxReverseLong(t *testing.T) {
	testSortBy(
		"sortBy(current,true)",
		"current",
		true,
		[]models.Series{
			getSeriesNamed("c", c),
			getSeriesNamed("d", d),
			getSeriesNamed("sumabc", sumabc),
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("a", a),
		},
		[]models.Series{
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("sumabc", sumabc),
			getSeriesNamed("a", a),
			getSeriesNamed("d", d),
			getSeriesNamed("c", c),
		},
		t,
	)
}

func testSortBy(name string, fn string, reverse bool, in []models.Series, out []models.Series, t *testing.T) {
	f := NewSortByConstructor(fn, reverse)()
	f.(*FuncSortBy).in = NewMock(in)

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

	dataMap := DataMap(make(map[Req][]models.Series))

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	/*
		TODO - Is sorting modification?
		t.Run("DidNotModifyInput", func(t *testing.T) {
			if err := equalOutput(inputCopy, in, nil, nil); err != nil {
				t.Fatalf("Case %s: Input was modified, err = %s", name, err)
			}
		})
	*/

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", name, err)
		}
	})
}

func BenchmarkSortBy10k_1NoNulls(b *testing.B) {
	benchmarkSortBy(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSortBy10k_10NoNulls(b *testing.B) {
	benchmarkSortBy(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSortBy10k_100NoNulls(b *testing.B) {
	benchmarkSortBy(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSortBy10k_1000NoNulls(b *testing.B) {
	benchmarkSortBy(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkSortBy10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSortBy(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSortBy10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSortBy(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSortBy10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSortBy(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSortBy10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSortBy(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkSortBy10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkSortBy(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSortBy10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkSortBy(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSortBy10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkSortBy(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSortBy10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkSortBy(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkSortBy(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f := NewSortByConstructor("average", true)()
		f.(*FuncSortBy).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
