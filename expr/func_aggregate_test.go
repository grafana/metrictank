package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestAggregateIdentity(t *testing.T) {
	testAggregate(
		"identity",
		"average",
		[][]models.Series{
			{
				getQuerySeries("single", a),
			},
		},
		getTargetSeries("averageSeries(single)", a),
		t,
		0,
	)
	testAggregate(
		"identity",
		"sum",
		[][]models.Series{
			{
				getQuerySeries("single", a),
			},
		},
		getTargetSeries("sumSeries(single)", a),
		t,
		0,
	)
}
func TestAggregateQueryToSingle(t *testing.T) {
	testAggregate(
		"query-to-single",
		"average",
		[][]models.Series{
			{
				getQuerySeries("foo.*", a),
			},
		},
		getTargetSeries("averageSeries(foo.*)", a),
		t,
		0,
	)
}
func TestAggregateMultiple(t *testing.T) {
	testAggregate(
		"avg-multiple-series",
		"average",
		[][]models.Series{
			{
				getQuerySeries("foo.*", a),
				getQuerySeries("foo.*", b),
			},
		},
		getTargetSeries("averageSeries(foo.*)", avgab),
		t,
		0,
	)
	testAggregate(
		"sum-multiple-series",
		"sum",
		[][]models.Series{
			{
				getQuerySeries("foo.*", a),
				getQuerySeries("foo.*", b),
			},
		},
		getTargetSeries("sumSeries(foo.*)", sumab),
		t,
		0,
	)
	testAggregate(
		"max-multiple-series",
		"max",
		[][]models.Series{
			{
				getQuerySeries("foo.*", a),
				getQuerySeries("foo.*", b),
			},
		},
		getTargetSeries("maxSeries(foo.*)", maxab),
		t,
		0,
	)
}
func TestAggregateMultipleDiffQuery(t *testing.T) {
	input := [][]models.Series{
		{
			getQuerySeries("foo.*", a),
			getQuerySeries("foo.*", b),
		},
		{
			getQuerySeries("movingAverage(bar, '1min')", c),
		},
	}

	testAggregate(
		"avg-multiple-serieslists",
		"average",
		input,
		getTargetSeries("averageSeries(foo.*,movingAverage(bar, '1min'))", avgabc),
		t,
		0,
	)
	testAggregate(
		"sum-multiple-serieslists",
		"sum",
		input,
		getTargetSeries("sumSeries(foo.*,movingAverage(bar, '1min'))", sumabc),
		t,
		0,
	)
	testAggregate(
		"max-multiple-serieslists",
		"max",
		input,
		getTargetSeries("maxSeries(foo.*,movingAverage(bar, '1min'))", maxabc),
		t,
		0,
	)
}

//mimic target=Aggregate(foo.*,foo.*,a,a)
func TestAggregateMultipleTimesSameInput(t *testing.T) {
	input := [][]models.Series{
		{
			getQuerySeries("foo.*", a),
			getQuerySeries("foo.*", b),
		},
		{
			getQuerySeries("foo.*", a),
			getQuerySeries("foo.*", b),
		},
		{
			getQuerySeries("a", a),
		},
		{
			getQuerySeries("a", a),
		},
	}
	testAggregate(
		"avg-multiple-times-same-input",
		"average",
		input,
		getTargetSeries("averageSeries(foo.*,foo.*,a,a)", avg4a2b),
		t,
		0,
	)
	testAggregate(
		"sum-multiple-times-same-input",
		"sum",
		input,
		getTargetSeries("sumSeries(foo.*,foo.*,a,a)", sum4a2b),
		t,
		0,
	)
}

func TestAggregateXFilesFactor(t *testing.T) {
	input := [][]models.Series{
		{
			getQuerySeries("foo.*", a),
			getQuerySeries("foo.*", b),
			getQuerySeries("foo.*", c),
		},
	}

	var avgabcxff05 = []schema.Point{
		{Val: 0, Ts: 10},
		{Val: math.MaxFloat64 / 3, Ts: 20},
		{Val: (math.MaxFloat64 - 13.5) / 3, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: float64(1234567893) / 2, Ts: 50},
		{Val: float64(1234567894) / 2, Ts: 60},
	}

	var avgabcxff075 = []schema.Point{
		{Val: 0, Ts: 10},
		{Val: math.MaxFloat64 / 3, Ts: 20},
		{Val: (math.MaxFloat64 - 13.5) / 3, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN() / 2, Ts: 50},
		{Val: math.NaN() / 2, Ts: 60},
	}

	testAggregate(
		"xFilesFactor-0",
		"average",
		input,
		getTargetSeries("averageSeries(foo.*)", avgabc),
		t,
		0,
	)
	testAggregate(
		"xFilesFactor-0.25",
		"average",
		input,
		getTargetSeries("averageSeries(foo.*)", avgabc),
		t,
		0.25,
	)

	testAggregate(
		"xFilesFactor-0.5",
		"average",
		input,
		getTargetSeries("averageSeries(foo.*)", avgabcxff05),
		t,
		0.5,
	)

	testAggregate(
		"xFilesFactor-0.75",
		"average",
		input,
		getTargetSeries("averageSeries(foo.*)", avgabcxff075),
		t,
		0.75,
	)

	testAggregate(
		"xFilesFactor-1",
		"average",
		input,
		getTargetSeries("averageSeries(foo.*)", avgabcxff075),
		t,
		1,
	)
}

func testAggregate(name, agg string, in [][]models.Series, out models.Series, t *testing.T, xFilesFactor float64) {
	f := NewAggregateConstructor(agg)()
	avg := f.(*FuncAggregate)
	for _, i := range in {
		avg.in = append(avg.in, NewMock(i))
	}
	avg.xFilesFactor = xFilesFactor
	got, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(got) != 1 {
		t.Fatalf("case %q: Aggregate output should be only 1 thing (a series) not %d", name, len(got))
	}
	g := got[0]
	if g.Target != out.Target {
		t.Fatalf("case %q: expected target %q, got %q", name, out.Target, g.Target)
	}
	if len(g.Datapoints) != len(out.Datapoints) {
		t.Fatalf("case %q: len output expected %d, got %d", name, len(out.Datapoints), len(g.Datapoints))
	}
	for j, p := range g.Datapoints {
		bothNaN := math.IsNaN(p.Val) && math.IsNaN(out.Datapoints[j].Val)
		if (bothNaN || p.Val == out.Datapoints[j].Val) && p.Ts == out.Datapoints[j].Ts {
			continue
		}
		t.Fatalf("case %q: output point %d - expected %v got %v", name, j, out.Datapoints[j], p)
	}
}

func BenchmarkAggregate10k_1NoNulls(b *testing.B) {
	benchmarkAggregate(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAggregate10k_10NoNulls(b *testing.B) {
	benchmarkAggregate(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAggregate10k_100NoNulls(b *testing.B) {
	benchmarkAggregate(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAggregate10k_1000NoNulls(b *testing.B) {
	benchmarkAggregate(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkAggregate10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAggregate(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAggregate10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAggregate(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAggregate10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAggregate(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAggregate10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAggregate(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkAggregate10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkAggregate(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAggregate10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkAggregate(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAggregate10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkAggregate(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAggregate10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkAggregate(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkAggregate(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: strconv.Itoa(i),
		}
		if i%1 == 0 {
			series.Datapoints = fn0()
		} else {
			series.Datapoints = fn1()
		}
		input = append(input, series)
	}
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		f := NewAggregateConstructor("average")()
		avg := f.(*FuncAggregate)
		avg.in = append(avg.in, NewMock(input))
		results, err = f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
	}
	b.SetBytes(int64(numSeries * len(results[0].Datapoints) * 12))
}
