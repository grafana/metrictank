package expr

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestConstantLineSimple(t *testing.T) {
	testConstantLine(
		"simple",
		1,
		0,
		100,
		makeConstantLineSeries(1, 0, 100),
		t,
	)
}

func makeConstantLineSeries(value float64, from uint32, to uint32) ([]models.Series) {
	series := []models.Series{
		{
			Target:		 fmt.Sprintf("%g", value),
			QueryPatt:   fmt.Sprintf("%g", value),
			Datapoints: []schema.Point{
				{Val: value, Ts: from},
				{Val: value, Ts: from + uint32((to-from)/2.0)},
				{Val: value, Ts: to},
			},
		},
	}

	return series
}

func testConstantLine(name string, value float64, from uint32, to uint32, out []models.Series, t *testing.T) {
	f := NewConstantLine()
	f.(*FuncConstantLine).value  = value
	f.(*FuncConstantLine).from = from
	f.(*FuncConstantLine).to = to
	gots, err := f.Exec(make(map[Req][]models.Series))

	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(gots) != len(out) {
		t.Fatalf("case %q: isNonNull len output expected %d, got %d", name, len(out), len(gots))
	}

	for i, g := range gots {
		exp := out[i]
		if g.QueryPatt != exp.QueryPatt {
			t.Fatalf("case %q: expected target %q, got %q", name, exp.QueryPatt, g.QueryPatt)
		}
		if len(g.Datapoints) != len(exp.Datapoints) {
			t.Fatalf("case %q: len output expected %d, got %d", name, len(exp.Datapoints), len(g.Datapoints))
		}
		for j, p := range g.Datapoints {
			if (p.Val == exp.Datapoints[j].Val) && p.Ts == exp.Datapoints[j].Ts {
				continue
			}
			t.Fatalf("case %q: output point %d - expected %v got %v", name, j, exp.Datapoints[j], p)
		}
	}
}

func BenchmarkConstantLine10k_1NoNulls(b *testing.B) {
	benchmarkConstantLine(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkConstantLine10k_10NoNulls(b *testing.B) {
	benchmarkConstantLine(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkConstantLine10k_100NoNulls(b *testing.B) {
	benchmarkConstantLine(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkConstantLine10k_1000NoNulls(b *testing.B) {
	benchmarkConstantLine(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkConstantLine10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkConstantLine(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewConstantLine()
		f.(*FuncConstantLine).value = 1.0
		f.(*FuncConstantLine).from = 1584849600
		f.(*FuncConstantLine).to = 1584849660
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
