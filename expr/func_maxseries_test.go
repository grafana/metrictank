package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"gopkg.in/raintank/schema.v1"
)

func TestMaxSeriesIdentity(t *testing.T) {
	testMaxSeries(
		"identity",
		[][]models.Series{
			{
				{
					QueryPatt:  "single",
					Target:     "single",
					Datapoints: getCopy(a),
				},
			},
		},
		models.Series{
			QueryPatt:  "maxSeries(single)",
			Datapoints: getCopy(a),
		},
		t,
	)
}
func TestMaxSeriesQueryToSingle(t *testing.T) {
	testMaxSeries(
		"query-to-single",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Target:     "foo",
					Datapoints: getCopy(a),
				},
			},
		},
		models.Series{
			QueryPatt:  "maxSeries(foo.*)",
			Datapoints: getCopy(a),
		},
		t,
	)
}
func TestMaxSeriesMultipleSameQuery(t *testing.T) {
	testMaxSeries(
		"max-multiple-series",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Target:     "foo.a",
					Datapoints: getCopy(a),
				},
				{
					QueryPatt:  "foo.*",
					Target:     "foo.b",
					Datapoints: getCopy(b),
				},
			},
		},
		models.Series{
			QueryPatt:  "maxSeries(foo.*)",
			Datapoints: getCopy(maxab),
		},
		t,
	)
}
func TestMaxSeriesMultipleDiffQuery(t *testing.T) {
	testMaxSeries(
		"max-multiple-serieslists",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Target:     "foo.a",
					Datapoints: getCopy(a),
				},
				{
					QueryPatt:  "foo.*",
					Target:     "foo.b",
					Datapoints: getCopy(b),
				},
			},
			{
				{
					QueryPatt:  "movingAverage(bar, '1min')",
					Target:     "movingAverage(bar, '1min')",
					Datapoints: getCopy(c),
				},
			},
		},
		models.Series{
			QueryPatt:  "maxSeries(foo.*,movingAverage(bar, '1min'))",
			Datapoints: getCopy(maxabc),
		},
		t,
	)
}

func testMaxSeries(name string, in [][]models.Series, out models.Series, t *testing.T) {
	f := NewMaxSeries()
	max := f.(*FuncMaxSeries)
	for _, i := range in {
		max.in = append(max.in, NewMock(i))
	}
	got, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(got) != 1 {
		t.Fatalf("case %q: maxSeries output should be only 1 thing (a series) not %d", name, len(got))
	}
	g := got[0]
	if g.QueryPatt != out.QueryPatt {
		t.Fatalf("case %q: expected target %q, got %q", name, out.QueryPatt, g.QueryPatt)
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

func BenchmarkMaxSeries10k_1NoNulls(b *testing.B) {
	benchmarkMaxSeries(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkMaxSeries10k_10NoNulls(b *testing.B) {
	benchmarkMaxSeries(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkMaxSeries10k_100NoNulls(b *testing.B) {
	benchmarkMaxSeries(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkMaxSeries10k_1000NoNulls(b *testing.B) {
	benchmarkMaxSeries(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkMaxSeries10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkMaxSeries(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMaxSeries10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkMaxSeries(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMaxSeries10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkMaxSeries(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMaxSeries10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkMaxSeries(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkMaxSeries10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkMaxSeries(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMaxSeries10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkMaxSeries(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMaxSeries10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkMaxSeries(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMaxSeries10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkMaxSeries(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkMaxSeries(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			QueryPatt: strconv.Itoa(i),
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
		f := NewMaxSeries()
		max := f.(*FuncMaxSeries)
		max.in = append(max.in, NewMock(input))
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
	b.SetBytes(int64(numSeries * len(input[0].Datapoints) * 12))
}
