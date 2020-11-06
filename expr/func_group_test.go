package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestGroup(t *testing.T) {
	testGroup("basic",
		[][]models.Series{
			{
				getSeries("a", "a", a),
			},
			{
				getSeries("b", "b", b),
			},
			{
				getSeries("abc", "abc", a),
				getSeries("abc", "abc", b),
				getSeries("abc", "abc", c),
			},
		},
		[]models.Series{
			getSeries("a", "a", a),
			getSeries("b", "b", b),
			getSeries("abc", "abc", a),
			getSeries("abc", "abc", b),
			getSeries("abc", "abc", c),
		},
		t)
}

func getNewGroup(in [][]models.Series) *FuncGroup {
	f := NewGroup()
	s := f.(*FuncGroup)
	for i := range in {
		s.in = append(s.in, NewMock(in[i]))
	}
	return s
}

func testGroup(name string, in [][]models.Series, out []models.Series, t *testing.T) {
	f := getNewGroup(in)

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
}

func BenchmarkGroup10k_1NoNulls(b *testing.B) {
	benchmarkGroup(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkGroup10k_10NoNulls(b *testing.B) {
	benchmarkGroup(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkGroup10k_100NoNulls(b *testing.B) {
	benchmarkGroup(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkGroup10k_1000NoNulls(b *testing.B) {
	benchmarkGroup(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkGroup10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkGroup(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewGroup()
		f.(*FuncGroup).in = append(f.(*FuncGroup).in, NewMock(input))
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
