package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"gopkg.in/raintank/schema.v1"
)

func TestDivideSeriesSingle(t *testing.T) {
	testDivideSeries(
		"single",
		[]models.Series{
			{
				Target:    "foo;a=a;b=b",
				QueryPatt: `seriesByTag("name=foo")`,
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
			},
		},
		[]models.Series{
			{
				Target:    "bar;a=a1;b=b",
				QueryPatt: `seriesByTag("name=bar")`,
				Datapoints: []schema.Point{
					{Val: 1, Ts: 10},
					{Val: 1, Ts: 20},
				},
			},
		},
		[]models.Series{
			{
				Target: "divideSeries(foo;a=a;b=b,bar;a=a1;b=b)",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
				Tags: map[string]string{
					"name": "divideSeries(foo;a=a;b=b,bar;a=a1;b=b)",
				},
			},
		},
		t,
	)
}

func TestDivideSeriesMultiple(t *testing.T) {
	testDivideSeries(
		"multiple",
		[]models.Series{
			{
				Target:    "foo-1;a=1;b=2;c=3",
				QueryPatt: "foo-1",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
			},
			{
				Target:    "foo-2;a=2;b=2;b=2",
				QueryPatt: "foo-2",
				Datapoints: []schema.Point{
					{Val: 20, Ts: 10},
					{Val: 100, Ts: 20},
				},
			},
		},
		[]models.Series{
			{
				Target:    "overbar;a=3;b=2;c=1",
				QueryPatt: "overbar",
				Datapoints: []schema.Point{
					{Val: 2, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
			},
		},
		[]models.Series{
			{
				Target: "divideSeries(foo-1;a=1;b=2;c=3,overbar;a=3;b=2;c=1)",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
				Tags: map[string]string{
					"name": "divideSeries(foo-1;a=1;b=2;c=3,overbar;a=3;b=2;c=1)",
				},
			},
			{
				Target: "divideSeries(foo-2;a=2;b=2;b=2,overbar;a=3;b=2;c=1)",
				Datapoints: []schema.Point{
					{Val: 10, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
				Tags: map[string]string{
					"name": "divideSeries(foo-2;a=2;b=2;b=2,overbar;a=3;b=2;c=1)",
				},
			},
		},
		t,
	)
}

func testDivideSeries(name string, dividend, divisor []models.Series, out []models.Series, t *testing.T) {
	f := NewDivideSeries()
	DivideSeries := f.(*FuncDivideSeries)
	DivideSeries.dividend = NewMock(dividend)
	DivideSeries.divisor = NewMock(divisor)
	got, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(got) != len(dividend) {
		t.Fatalf("case %q: DivideSeries output should be same amount of series as dividend input: %d, not %d", name, len(dividend), len(got))
	}
	for i, o := range out {
		g := got[i]
		if o.Target != g.Target {
			t.Fatalf("case %q: expected target %q, got %q", name, o.Target, g.Target)
		}
		if len(o.Datapoints) != len(g.Datapoints) {
			t.Fatalf("case %q: len output expected %d, got %d", name, len(o.Datapoints), len(g.Datapoints))
		}
		for j, p := range o.Datapoints {
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(g.Datapoints[j].Val)
			if (bothNaN || p.Val == g.Datapoints[j].Val) && p.Ts == g.Datapoints[j].Ts {
				continue
			}
			t.Fatalf("case %q: output point %d - expected %v got %v", name, j, p, g.Datapoints[j])
		}
		if len(o.Tags) != len(g.Tags) {
			t.Fatalf("case %q: len tags expected %d, got %d", name, len(o.Tags), len(g.Tags))
		}
		for k, v := range g.Tags {
			if o.Tags[k] == v {
				continue
			}
			t.Fatalf("case %q: output tag %q different, expected %q but got %q", name, k, o.Tags[k], v)
		}
	}
}

func BenchmarkDivideSeries10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkDivideSeries(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDivideSeries10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkDivideSeries(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDivideSeries10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkDivideSeries(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDivideSeries10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkDivideSeries(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkDivideSeries(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
	var dividends []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: strconv.Itoa(i),
		}
		if i%1 == 0 {
			series.Datapoints = fn0()
		} else {
			series.Datapoints = fn1()
		}
		dividends = append(dividends, series)
	}
	divisor := models.Series{
		Target:     "divisor",
		Datapoints: fn0(),
	}
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		f := NewDivideSeries()
		DivideSeries := f.(*FuncDivideSeries)
		DivideSeries.dividend = NewMock(dividends)
		DivideSeries.divisor = NewMock([]models.Series{divisor})
		results, err = f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
	}
	b.SetBytes(int64(numSeries * len(results[0].Datapoints) * 12))
}
