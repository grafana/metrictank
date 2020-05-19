package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

var aWith0 = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 5.5, Ts: 30},
	{Val: 0, Ts: 40},
	{Val: 0, Ts: 50},
	{Val: 1234567890, Ts: 60},
}

func TestTransformNullNoInput(t *testing.T) {
	testTransformNull("no_input", 0, []models.Series{}, []models.Series{}, t)
}

func TestTransformNullSingle(t *testing.T) {
	testTransformNull(
		"single",
		0,
		[]models.Series{
			getSeriesNamed("a", a),
		},
		[]models.Series{
			getSeriesNamed("transformNull(a,0)", aWith0),
		},
		t,
	)
}

func TestTransformNullMultiple(t *testing.T) {
	testTransformNull(
		"single",
		0,
		[]models.Series{
			getSeriesNamed("a", a),
			getSeriesNamed("a2", a),
		},
		[]models.Series{
			getSeriesNamed("transformNull(a,0)", aWith0),
			getSeriesNamed("transformNull(a2,0)", aWith0),
		},
		t,
	)
}

func testTransformNull(name string, def float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewTransformNull()
	f.(*FuncTransformNull).in = NewMock(in)
	f.(*FuncTransformNull).def = def

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

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

func BenchmarkTransformNull10k_1NoNulls(b *testing.B) {
	benchmarkTransformNull(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkTransformNull10k_10NoNulls(b *testing.B) {
	benchmarkTransformNull(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkTransformNull10k_100NoNulls(b *testing.B) {
	benchmarkTransformNull(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkTransformNull10k_1000NoNulls(b *testing.B) {
	benchmarkTransformNull(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkTransformNull10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkTransformNull(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTransformNull10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkTransformNull(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTransformNull10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkTransformNull(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTransformNull10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkTransformNull(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkTransformNull10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkTransformNull(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTransformNull10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkTransformNull(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTransformNull10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkTransformNull(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTransformNull10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkTransformNull(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkTransformNull(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewTransformNull()
		f.(*FuncTransformNull).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
