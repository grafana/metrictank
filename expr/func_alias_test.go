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
				QueryPatt: "bar",
				Target:    "bar",
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
				QueryPatt: "bar",
				Target:    "bar",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
			},
			{
				QueryPatt: "bar",
				Target:    "bar",
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
	alias := f.(*FuncAlias)
	alias.alias = "bar"
	alias.in = NewMock(in)
	got, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(got) != len(in) {
		t.Fatalf("case %q: alias output should be same amount of series as input: %d, not %d", name, len(in), len(got))
	}
	for i, _ := range got {
		if err := equalSeries(out[i], got[i]); err != nil {
			t.Fatalf("case %q: %s", name, err)
		}
	}
}

func BenchmarkAlias_1(b *testing.B) {
	benchmarkAlias(b, 1)
}
func BenchmarkAlias_10(b *testing.B) {
	benchmarkAlias(b, 10)
}
func BenchmarkAlias_100(b *testing.B) {
	benchmarkAlias(b, 100)
}
func BenchmarkAlias_1000(b *testing.B) {
	benchmarkAlias(b, 1000)
}

func benchmarkAlias(b *testing.B, numSeries int) {
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
		alias := f.(*FuncAlias)
		alias.alias = "new-name"
		alias.in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
