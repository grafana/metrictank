package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

var aIsNonNull = []schema.Point{
	{Val: 1, Ts: 10},
	{Val: 1, Ts: 20},
	{Val: 1, Ts: 30},
	{Val: 0, Ts: 40},
	{Val: 0, Ts: 50},
	{Val: 1, Ts: 60},
}

var bIsNonNull = []schema.Point{
	{Val: 1, Ts: 10},
	{Val: 1, Ts: 20},
	{Val: 1, Ts: 30},
	{Val: 0, Ts: 40},
	{Val: 1, Ts: 50},
	{Val: 0, Ts: 60},
}

var cdIsNonNull = []schema.Point{
	{Val: 1, Ts: 10},
	{Val: 1, Ts: 20},
	{Val: 1, Ts: 30},
	{Val: 1, Ts: 40},
	{Val: 1, Ts: 50},
	{Val: 1, Ts: 60},
}

func TestIsNonNullSingle(t *testing.T) {
	testIsNonNull(
		"identity",
		[]models.Series{
			getQuerySeries("a", a),
		},
		[]models.Series{
			getQuerySeries("isNonNull(a)", aIsNonNull),
		},
		t,
	)
}

func TestIsNonNullSingleAllNonNull(t *testing.T) {
	testIsNonNull(
		"identity-counter8bit",
		[]models.Series{
			getQuerySeries("counter8bit", d),
		},
		[]models.Series{
			getQuerySeries("isNonNull(counter8bit)", cdIsNonNull),
		},
		t,
	)
}

func TestIsNonNullMulti(t *testing.T) {
	testIsNonNull(
		"multiple-series",
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("b.*", b),
			getQuerySeries("c.foo{bar,baz}", c),
			getQuerySeries("movingAverage(bar, '1min')", d),
		},
		[]models.Series{
			getQuerySeries("isNonNull(a)", aIsNonNull),
			getQuerySeries("isNonNull(b.*)", bIsNonNull),
			getQuerySeries("isNonNull(c.foo{bar,baz})", cdIsNonNull),
			getQuerySeries("isNonNull(movingAverage(bar, '1min'))", cdIsNonNull),
		},
		t,
	)
}

func testIsNonNull(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := NewIsNonNull()
	f.(*FuncIsNonNull).in = NewMock(in)

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

func BenchmarkIsNonNull10k_1NoNulls(b *testing.B) {
	benchmarkIsNonNull(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkIsNonNull10k_10NoNulls(b *testing.B) {
	benchmarkIsNonNull(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkIsNonNull10k_100NoNulls(b *testing.B) {
	benchmarkIsNonNull(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkIsNonNull10k_1000NoNulls(b *testing.B) {
	benchmarkIsNonNull(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkIsNonNull10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkIsNonNull(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIsNonNull10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkIsNonNull(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIsNonNull10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkIsNonNull(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIsNonNull10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkIsNonNull(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkIsNonNull10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkIsNonNull(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIsNonNull10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkIsNonNull(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIsNonNull10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkIsNonNull(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkIsNonNull10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkIsNonNull(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkIsNonNull(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewIsNonNull()
		f.(*FuncIsNonNull).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
