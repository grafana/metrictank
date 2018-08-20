package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
)

func TestKeepLastValueAll(t *testing.T) {
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5, Ts: 30},
		{Val: 5.5, Ts: 40},
		{Val: 5.5, Ts: 50},
		{Val: 1234567890, Ts: 60},
	}

	testKeepLastValue(
		"keepAll",
		math.MaxInt64,
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "keepLastValue(a)",
				Datapoints: out,
			},
		},
		t,
	)
}

func TestKeepLastValueNone(t *testing.T) {

	testKeepLastValue(
		"keepNone",
		0,
		[]models.Series{
			{
				Interval:   10,
				Target:     "sum4a2b",
				Datapoints: getCopy(sum4a2b),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "keepLastValue(sum4a2b)",
				Datapoints: getCopy(sum4a2b),
			},
		},
		t,
	)
}

func TestKeepLastValueOne(t *testing.T) {
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: math.MaxFloat64, Ts: 20},
		{Val: math.MaxFloat64 - 20, Ts: 30},
		{Val: math.MaxFloat64 - 20, Ts: 40},
		{Val: 1234567890, Ts: 50},
		{Val: 1234567890, Ts: 60},
	}

	testKeepLastValue(
		"keepOne",
		1,
		[]models.Series{
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "keepLastValue(b)",
				Datapoints: out,
			},
			{
				Interval:   10,
				Target:     "keepLastValue(a)",
				Datapoints: getCopy(a),
			},
		},
		t,
	)
}

func testKeepLastValue(name string, limit int64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewKeepLastValue()
	f.(*FuncKeepLastValue).in = NewMock(in)
	f.(*FuncKeepLastValue).limit = limit
	gots, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q (%d): err should be nil. got %q", name, limit, err)
	}
	if len(gots) != len(out) {
		t.Fatalf("case %q (%d): isNonNull len output expected %d, got %d", name, limit, len(out), len(gots))
	}
	for i, g := range gots {
		exp := out[i]
		if g.Target != exp.Target {
			t.Fatalf("case %q (%d): expected target %q, got %q", name, limit, exp.Target, g.Target)
		}
		if len(g.Datapoints) != len(exp.Datapoints) {
			t.Fatalf("case %q (%d) len output expected %d, got %d", name, limit, len(exp.Datapoints), len(g.Datapoints))
		}
		for j, p := range g.Datapoints {
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(exp.Datapoints[j].Val)
			if (bothNaN || p.Val == exp.Datapoints[j].Val) && p.Ts == exp.Datapoints[j].Ts {
				continue
			}
			t.Fatalf("case %q (%d): output point %d - expected %v got %v", name, limit, j, exp.Datapoints[j], p)
		}
	}
}

func BenchmarkKeepLastValue10k_1NoNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkKeepLastValue10k_10NoNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkKeepLastValue10k_100NoNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkKeepLastValue10k_1000NoNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkKeepLastValue10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkKeepLastValue10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkKeepLastValue10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkKeepLastValue(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkKeepLastValue(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewKeepLastValue()
		f.(*FuncKeepLastValue).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
