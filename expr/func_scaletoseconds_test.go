package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestScaleToSecondsSingle(t *testing.T) {
	testScaleToSeconds(
		"identity",
		[]models.Series{
			getSeries("a", "a", a),
		},
		[]models.Series{
			getSeriesNamed("scaleToSeconds(a,10)", a),
		},
		t,
		10,
	)
}

func TestScaleToSecondsSingleAllNonNull(t *testing.T) {
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 3.0437127721620759e+19, Ts: 20},
		{Val: 1.8354510353341003e+20, Ts: 30},
		{Val: 2.674777890687885e+19, Ts: 40},
		{Val: 7.3786976294838198e+19, Ts: 50},
		{Val: 2.3058430092136936e+20, Ts: 60},
	}

	testScaleToSeconds(
		"identity-largeseconds",
		[]models.Series{
			getSeries("d", "d", d),
		},
		[]models.Series{
			getSeriesNamed("scaleToSeconds(d,9223372036854774784)", out),
		},
		t,
		9223372036854774784,
	)
}

func TestScaleToSecondsMulti(t *testing.T) {
	out1 := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: math.Inf(0), Ts: 20},
		{Val: math.Inf(0), Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: 123456.7890, Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	out2 := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 0.0001, Ts: 30},
		{Val: 0.0002, Ts: 40},
		{Val: 0.0003, Ts: 50},
		{Val: 0.0004, Ts: 60},
	}
	testScaleToSeconds(
		"multiple-series-subseconds",
		[]models.Series{
			getSeries("b.*", "b.*", b),
			getSeries("c.foo{bar,baz}", "c.foo{bar,baz}", c),
		},
		[]models.Series{
			getSeriesNamed("scaleToSeconds(b.*,0)", out1),
			getSeriesNamed("scaleToSeconds(c.foo{bar,baz},0)", out2),
		},
		t,
		0.001,
	)
}

func testScaleToSeconds(name string, in []models.Series, out []models.Series, t *testing.T, seconds float64) {
	f := NewScaleToSeconds()
	f.(*FuncScaleToSeconds).in = NewMock(in)
	f.(*FuncScaleToSeconds).seconds = seconds

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

func BenchmarkScaleToSeconds10k_1NoNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkScaleToSeconds10k_10NoNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkScaleToSeconds10k_100NoNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkScaleToSeconds10k_1000NoNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkScaleToSeconds10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScaleToSeconds10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScaleToSeconds10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScaleToSeconds10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkScaleToSeconds10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScaleToSeconds10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScaleToSeconds10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkScaleToSeconds10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkScaleToSeconds(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkScaleToSeconds(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewScaleToSeconds()
		f.(*FuncScaleToSeconds).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
