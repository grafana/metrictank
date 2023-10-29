package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/pkg/api/models"
)

func TestAliasZero(t *testing.T) {
	testAlias("zero", []models.Series{}, []models.Series{}, t)
}
func TestAliasSingle(t *testing.T) {
	testAlias(
		"single",
		[]models.Series{
			getSeriesNamed("foo", a),
		},
		[]models.Series{
			getSeriesNamed("bar", a),
		},
		t,
	)
}
func TestAliasMultiple(t *testing.T) {
	testAlias(
		"multiple",
		[]models.Series{
			getSeriesNamed("foo-1", a),
			getSeriesNamed("foo-2", b),
		},
		[]models.Series{
			getSeriesNamed("bar", a),
			getSeriesNamed("bar", b),
		},
		t,
	)
}

func makeAlias(in []models.Series) GraphiteFunc {
	f := NewAlias()
	alias := f.(*FuncAlias)
	alias.alias = "bar"
	alias.in = NewMock(in)
	return f
}

func testAlias(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := makeAlias(in)

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

func BenchmarkAlias_1(b *testing.B) {
	benchmarkAlias(b, 1)
}
func BenchmarkAlias_10(b *testing.B) {
	benchmarkAlias(b, 10)
}
func BenchmarkAlias_100(b *testing.B) {
	benchmarkAlias(b, 100)
}
func BenchmarkAlias_1000(b *testing.B) {
	benchmarkAlias(b, 1000)
}

func benchmarkAlias(b *testing.B, numSeries int) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: strconv.Itoa(i),
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewAlias()
		alias := f.(*FuncAlias)
		alias.alias = "new-name"
		alias.in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
