package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

func TestAliasSingle(t *testing.T) {
	testAlias(
		"single",
		[]models.Series{
			{
				Target: "foo",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
			},
		},
		[]models.Series{
			{
				Target: "bar",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
			},
		},
		t,
	)
}
func TestAliasMultiple(t *testing.T) {
	testAlias(
		"multiple",
		[]models.Series{
			{
				Target: "foo-1",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
			},
			{
				Target: "foo-2",
				Datapoints: []schema.Point{
					{Val: 20, Ts: 10},
					{Val: 100, Ts: 20},
				},
			},
		},
		[]models.Series{
			{
				Target: "bar",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
			},
			{
				Target: "bar",
				Datapoints: []schema.Point{
					{Val: 20, Ts: 10},
					{Val: 100, Ts: 20},
				},
			},
		},
		t,
	)
}

func testAlias(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := NewAlias()
	got, err := f.Exec(make(map[Req][]models.Series), interface{}(in), interface{}("bar"))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(got) != len(in) {
		t.Fatalf("case %q: alias output should be same amount of series as input: %d, not %d", name, len(in), len(got))
	}
	for i, o := range out {
		g, ok := got[i].(models.Series)
		if !ok {
			t.Fatalf("case %q: expected alias output of models.Series type", name)
		}
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
	}
}

func BenchmarkAlias1M_1(b *testing.B) {
	benchmarkAlias1M(b, 1)
}
func BenchmarkAlias1M_10(b *testing.B) {
	benchmarkAlias1M(b, 10)
}
func BenchmarkAlias1M_100(b *testing.B) {
	benchmarkAlias1M(b, 100)
}
func BenchmarkAlias1M_1000(b *testing.B) {
	benchmarkAlias1M(b, 1000)
}

func benchmarkAlias1M(b *testing.B, numSeries int) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: strconv.Itoa(i),
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewAlias()
		got, err := f.Exec(make(map[Req][]models.Series), interface{}(input), "new-name")
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
