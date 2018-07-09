package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"gopkg.in/raintank/schema.v1"
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
			{
				Interval:   10,
				QueryPatt:  "a",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "isNonNull(a)",
				Datapoints: getCopy(aIsNonNull),
			},
		},
		t,
	)
}

func TestIsNonNullSingleAllNonNull(t *testing.T) {
	testIsNonNull(
		"identity-counter8bit",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "counter8bit",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "isNonNull(counter8bit)",
				Datapoints: getCopy(cdIsNonNull),
			},
		},
		t,
	)
}

func TestIsNonNullMulti(t *testing.T) {
	testIsNonNull(
		"multiple-series",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "b.*",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "c.foo{bar,baz}",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "movingAverage(bar, '1min')",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				QueryPatt:  "isNonNull(a)",
				Datapoints: getCopy(aIsNonNull),
			},
			{
				QueryPatt:  "isNonNull(b.*)",
				Datapoints: getCopy(bIsNonNull),
			},
			{
				QueryPatt:  "isNonNull(c.foo{bar,baz})",
				Datapoints: getCopy(cdIsNonNull),
			},
			{
				QueryPatt:  "isNonNull(movingAverage(bar, '1min'))",
				Datapoints: getCopy(cdIsNonNull),
			},
		},
		t,
	)
}

func testIsNonNull(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := NewIsNonNull()
	f.(*FuncIsNonNull).in = NewMock(in)
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
