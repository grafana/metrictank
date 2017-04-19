package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/test"
	"gopkg.in/raintank/schema.v1"
)

func TestSumSeriesIdentity(t *testing.T) {
	testSumSeries(
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
			Target:     "sumSeries(single)",
			Datapoints: getCopy(a),
		},
		t,
	)
}
func TestSumSeriesQueryToSingle(t *testing.T) {
	testSumSeries(
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
			Target:     "sumSeries(foo.*)",
			Datapoints: getCopy(a),
		},
		t,
	)
}
func TestSumSeriesMultipleSameQuery(t *testing.T) {
	testSumSeries(
		"sum-multiple-series",
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
			Target:     "sumSeries(foo.*)",
			Datapoints: getCopy(sumab),
		},
		t,
	)
}
func TestSumSeriesMultipleDiffQuery(t *testing.T) {
	testSumSeries(
		"sum-multiple-serieslists",
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
			Target:     "sumSeries(foo.*,movingAverage(bar, '1min'))",
			Datapoints: getCopy(sumabc),
		},
		t,
	)
}

func testSumSeries(name string, in [][]models.Series, out models.Series, t *testing.T) {
	f := NewSumSeries()
	var input []interface{}
	for _, i := range in {
		input = append(input, i)
	}
	got, err := f.Exec(make(map[Req][]models.Series), input...)
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(got) != 1 {
		t.Fatalf("case %q: sumSeries output should be only 1 thing (a series) not %d", name, len(got))
	}
	g, ok := got[0].(models.Series)
	if !ok {
		t.Fatalf("case %q: expected sum output of models.Series type", name)
	}
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

func BenchmarkSumSeries10k_1NoNulls(b *testing.B) {
	benchmarkSumSeries(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSumSeries10k_10NoNulls(b *testing.B) {
	benchmarkSumSeries(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSumSeries10k_100NoNulls(b *testing.B) {
	benchmarkSumSeries(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSumSeries10k_1000NoNulls(b *testing.B) {
	benchmarkSumSeries(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkSumSeries10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSumSeries(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSumSeries10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSumSeries(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSumSeries10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSumSeries(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSumSeries10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSumSeries(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkSumSeries10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkSumSeries(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSumSeries10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkSumSeries(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSumSeries10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkSumSeries(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSumSeries10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkSumSeries(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkSumSeries(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
	for i := 0; i < b.N; i++ {
		f := NewSumSeries()
		got, err := f.Exec(make(map[Req][]models.Series), interface{}(input))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
	b.SetBytes(int64(numSeries * len(input[0].Datapoints) * 12))
}
