package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"gopkg.in/raintank/schema.v1"
)

func TestAvgSeriesIdentity(t *testing.T) {
	testAvgSeries(
		"identity",
		[][]models.Series{
			{
				{
					QueryPatt:  "single",
					Datapoints: getCopy(a),
				},
			},
		},
		models.Series{
			Target:     "averageSeries(single)",
			Datapoints: getCopy(a),
		},
		t,
	)
}
func TestAvgSeriesQueryToSingle(t *testing.T) {
	testAvgSeries(
		"query-to-single",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(a),
				},
			},
		},
		models.Series{
			Target:     "averageSeries(foo.*)",
			Datapoints: getCopy(a),
		},
		t,
	)
}
func TestAvgSeriesMultiple(t *testing.T) {
	testAvgSeries(
		"avg-multiple-series",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(a),
				},
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(b),
				},
			},
		},
		models.Series{
			Target:     "averageSeries(foo.*)",
			Datapoints: getCopy(avgab),
		},
		t,
	)
}
func TestAvgSeriesMultipleDiffQuery(t *testing.T) {
	testAvgSeries(
		"avg-multiple-serieslists",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(a),
				},
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(b),
				},
			},
			{
				{
					QueryPatt:  "movingAverage(bar, '1min')",
					Datapoints: getCopy(c),
				},
			},
		},
		models.Series{
			Target:     "averageSeries(foo.*,movingAverage(bar, '1min'))",
			Datapoints: getCopy(avgabc),
		},
		t,
	)
}

//mimic target=avgSeries(foo.*,foo.*,a,a)
func TestAvgSeriesMultipleTimesSameInput(t *testing.T) {
	testAvgSeries(
		"avg-multiple-times-same-input",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(a),
				},
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(b),
				},
			},
			{
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(a),
				},
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(b),
				},
			},
			{
				{
					QueryPatt:  "a",
					Datapoints: getCopy(a),
				},
			},
			{
				{
					QueryPatt:  "a",
					Datapoints: getCopy(a),
				},
			},
		},
		models.Series{
			Target:     "averageSeries(foo.*,foo.*,a,a)",
			Datapoints: getCopy(avg4a2b),
		},
		t,
	)
}

func testAvgSeries(name string, in [][]models.Series, out models.Series, t *testing.T) {
	f := NewAvgSeries()
	avg := f.(*FuncAvgSeries)
	for _, i := range in {
		avg.in = append(avg.in, NewMock(i))
	}
	got, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(got) != 1 {
		t.Fatalf("case %q: avgSeries output should be only 1 thing (a series) not %d", name, len(got))
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

func BenchmarkAvgSeries10k_1NoNulls(b *testing.B) {
	benchmarkAvgSeries(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAvgSeries10k_10NoNulls(b *testing.B) {
	benchmarkAvgSeries(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAvgSeries10k_100NoNulls(b *testing.B) {
	benchmarkAvgSeries(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAvgSeries10k_1000NoNulls(b *testing.B) {
	benchmarkAvgSeries(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkAvgSeries10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAvgSeries(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAvgSeries10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAvgSeries(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAvgSeries10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAvgSeries(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAvgSeries10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAvgSeries(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkAvgSeries10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkAvgSeries(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAvgSeries10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkAvgSeries(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAvgSeries10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkAvgSeries(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAvgSeries10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkAvgSeries(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkAvgSeries(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewAvgSeries()
		avg := f.(*FuncAvgSeries)
		avg.in = append(avg.in, NewMock(input))
		results, err = f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
	}
	b.SetBytes(int64(numSeries * len(results[0].Datapoints) * 12))
}
