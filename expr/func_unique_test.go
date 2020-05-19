package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func getNewUnique(in [][]models.Series) *FuncUnique {
	f := NewUnique()
	s := f.(*FuncUnique)
	for i := range in {
		s.in = append(s.in, NewMock(in[i]))
	}
	return s
}

func TestUnique(t *testing.T) {
	testUnique(
		"basic",
		[][]models.Series{
			{
				getSeries("foo.a", "foo.a", a),
				getSeries("foo.b", "foo.b", b),
			},
			{
				getSeries("bar.b", "bar.b", b),
			},
			{
				getSeries("foo.a", "foo.a", a),
				getSeries("bar.b", "bar.*", b),
				getSeries("bar.d", "bar.*", d),
				getSeries("foo.a", "foo.a", a),
			},
		}, []models.Series{
			getSeries("foo.a", "foo.a", a),
			getSeries("foo.b", "foo.b", b),
			getSeries("bar.b", "bar.b", b),
			getSeries("bar.d", "bar.*", d),
		},
		t)
}

func testUnique(name string, in [][]models.Series, out []models.Series, t *testing.T) {
	f := getNewUnique(in)

	// Copy input to check that it is unchanged later
	inputCopy := make([][]models.Series, len(in))
	for i := range in {
		inputCopy[i] = make([]models.Series, len(in[i]))
		copy(inputCopy[i], in[i])
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

func BenchmarkUnique10k_1NoNulls(b *testing.B) {
	benchmarkUnique(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkUnique10k_10NoNulls(b *testing.B) {
	benchmarkUnique(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkUnique10k_100NoNulls(b *testing.B) {
	benchmarkUnique(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkUnique10k_1000NoNulls(b *testing.B) {
	benchmarkUnique(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkUnique10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkUnique(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkUnique10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkUnique(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkUnique10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkUnique(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkUnique10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkUnique(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkUnique10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkUnique(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkUnique10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkUnique(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkUnique10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkUnique(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkUnique10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkUnique(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkUnique(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewUnique()
		f.(*FuncUnique).in = append(f.(*FuncUnique).in, NewMock(input))
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
