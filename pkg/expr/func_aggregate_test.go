package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/internal/test"
	"github.com/grafana/metrictank/pkg/api/models"
)

func TestAggregateZero(t *testing.T) {
	f := makeAggregate("sum", [][]models.Series{}, 0)
	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput([]models.Series{}, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestAggregateIdentity(t *testing.T) {
	testAggregate(
		"identity",
		"average",
		[][]models.Series{
			{
				getSeriesNamed("single", a),
			},
		},
		getSeriesNamed("averageSeries(single)", a),
		t,
		0,
	)
	testAggregate(
		"identity",
		"sum",
		[][]models.Series{
			{
				getSeriesNamed("single", a),
			},
		},
		getSeriesNamed("sumSeries(single)", a),
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
				getSeriesNamed("foo.*", a),
			},
		},
		getSeriesNamed("averageSeries(foo.*)", a),
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
				getSeriesNamed("foo.*", a),
				getSeriesNamed("foo.*", b),
			},
		},
		getSeriesNamed("averageSeries(foo.*)", avgab),
		t,
		0,
	)
	testAggregate(
		"sum-multiple-series",
		"sum",
		[][]models.Series{
			{
				getSeriesNamed("foo.*", a),
				getSeriesNamed("foo.*", b),
			},
		},
		getSeriesNamed("sumSeries(foo.*)", sumab),
		t,
		0,
	)
	testAggregate(
		"max-multiple-series",
		"max",
		[][]models.Series{
			{
				getSeriesNamed("foo.*", a),
				getSeriesNamed("foo.*", b),
			},
		},
		getSeriesNamed("maxSeries(foo.*)", maxab),
		t,
		0,
	)
}
func TestAggregateMultipleDiffQuery(t *testing.T) {
	input := [][]models.Series{
		{
			getSeriesNamed("foo.*", a),
			getSeriesNamed("foo.*", b),
		},
		{
			getSeriesNamed("movingAverage(bar, '1min')", c),
		},
	}

	testAggregate(
		"avg-multiple-serieslists",
		"average",
		input,
		getSeriesNamed("averageSeries(foo.*,movingAverage(bar, '1min'))", avgabc),
		t,
		0,
	)
	testAggregate(
		"sum-multiple-serieslists",
		"sum",
		input,
		getSeriesNamed("sumSeries(foo.*,movingAverage(bar, '1min'))", sumabc),
		t,
		0,
	)
	testAggregate(
		"max-multiple-serieslists",
		"max",
		input,
		getSeriesNamed("maxSeries(foo.*,movingAverage(bar, '1min'))", maxabc),
		t,
		0,
	)
}

// mimic target=Aggregate(foo.*,foo.*,a,a)
func TestAggregateMultipleTimesSameInput(t *testing.T) {
	input := [][]models.Series{
		{
			getSeriesNamed("foo.*", a),
			getSeriesNamed("foo.*", b),
		},
		{
			getSeriesNamed("foo.*", a),
			getSeriesNamed("foo.*", b),
		},
		{
			getSeriesNamed("a", a),
		},
		{
			getSeriesNamed("a", a),
		},
	}
	testAggregate(
		"avg-multiple-times-same-input",
		"average",
		input,
		getSeriesNamed("averageSeries(foo.*,foo.*,a,a)", avg4a2b),
		t,
		0,
	)
	testAggregate(
		"sum-multiple-times-same-input",
		"sum",
		input,
		getSeriesNamed("sumSeries(foo.*,foo.*,a,a)", sum4a2b),
		t,
		0,
	)
}

func TestAggregateXFilesFactor(t *testing.T) {
	input := [][]models.Series{
		{
			getSeriesNamed("foo.*", a),
			getSeriesNamed("foo.*", b),
			getSeriesNamed("foo.*", c),
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
		getSeriesNamed("averageSeries(foo.*)", avgabc),
		t,
		0,
	)
	testAggregate(
		"xFilesFactor-0.25",
		"average",
		input,
		getSeriesNamed("averageSeries(foo.*)", avgabc),
		t,
		0.25,
	)

	testAggregate(
		"xFilesFactor-0.5",
		"average",
		input,
		getSeriesNamed("averageSeries(foo.*)", avgabcxff05),
		t,
		0.5,
	)

	testAggregate(
		"xFilesFactor-0.75",
		"average",
		input,
		getSeriesNamed("averageSeries(foo.*)", avgabcxff075),
		t,
		0.75,
	)

	testAggregate(
		"xFilesFactor-1",
		"average",
		input,
		getSeriesNamed("averageSeries(foo.*)", avgabcxff075),
		t,
		1,
	)
}

func makeAggregate(agg string, in [][]models.Series, xFilesFactor float64) GraphiteFunc {
	f := NewAggregateConstructor(agg)()
	avg := f.(*FuncAggregate)
	for _, i := range in {
		avg.in = append(avg.in, NewMock(i))
	}
	avg.xFilesFactor = xFilesFactor
	return f
}

func testAggregate(name, agg string, in [][]models.Series, out models.Series, t *testing.T, xFilesFactor float64) {
	inputCopy := make([][]models.Series, len(in)) // to later verify that it is unchanged
	for i := range in {
		inputCopy[i] = models.SeriesCopy(in[i])
	}

	f := makeAggregate(agg, in, xFilesFactor)

	dataMap := initDataMapMultiple(in)

	got, err := f.Exec(dataMap)
	if err := equalOutput([]models.Series{out}, got, nil, err); err != nil {
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

func benchmarkAggregate(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: strconv.Itoa(i),
		}
		if i%1 == 0 {
			series.Datapoints, series.Interval = fn0()
		} else {
			series.Datapoints, series.Interval = fn1()
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
