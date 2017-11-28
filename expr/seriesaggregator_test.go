package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	schema "gopkg.in/raintank/schema.v1"
)

func TestSeriesAggregatorsIdentity(t *testing.T) {
	input := []models.Series{
		{
			QueryPatt:  "single",
			Datapoints: getCopy(a),
		},
	}
	testSeriesAggregate(
		"identity",
		"average",
		input,
		getCopy(a),
		t,
	)
	testSeriesAggregate(
		"identity",
		"sum",
		input,
		getCopy(a),
		t,
	)
	testSeriesAggregate(
		"identity",
		"max",
		input,
		getCopy(a),
		t,
	)
	testSeriesAggregate(
		"identity",
		"min",
		input,
		getCopy(a),
		t,
	)
}

func TestSeriesAggregateMultiple(t *testing.T) {
	input := []models.Series{
		{
			QueryPatt:  "foo.*",
			Datapoints: getCopy(a),
		},
		{
			QueryPatt:  "foo.*",
			Datapoints: getCopy(b),
		},
		{
			QueryPatt:  "movingAverage(bar, '1min')",
			Datapoints: getCopy(c),
		},
	}

	testSeriesAggregate(
		"multipleSeries",
		"average",
		input,
		getCopy(avgabc),
		t,
	)
	testSeriesAggregate(
		"multipleSeries",
		"sum",
		input,
		getCopy(sumabc),
		t,
	)
	testSeriesAggregate(
		"multipleSeries",
		"max",
		input,
		getCopy(maxabc),
		t,
	)
	testSeriesAggregate(
		"multipleSeries",
		"min",
		input,
		getCopy(minabc),
		t,
	)
}

func testSeriesAggregate(name, agg string, in []models.Series, out []schema.Point, t *testing.T) {
	f := getCrossSeriesAggFunc(agg)

	got := make([]schema.Point, 0, len(out))
	f(in, &got)

	if len(got) != len(out) {
		t.Fatalf("case %q (%q): len output expected %d, got %d", name, agg, len(out), len(got))
	}
	for j, p := range got {
		bothNaN := math.IsNaN(p.Val) && math.IsNaN(out[j].Val)
		if (bothNaN || p.Val == out[j].Val) && p.Ts == out[j].Ts {
			continue
		}
		t.Fatalf("case %q: output point %d - expected %v got %v", name, j, out[j], p)
	}
}

func BenchmarkSeriesAggregateAvg10k_100NoNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesAvg, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSeriesAggregateAvg10k_100WithNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesAvg, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkSeriesAggregateSum10k_100NoNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesSum, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSeriesAggregateSum10k_100WithNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesSum, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkSeriesAggregateMax10k_100NoNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesMax, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSeriesAggregateMax10k_100WithNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesMax, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkSeriesAggregateMin10k_100NoNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesMin, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSeriesAggregateMin10k_100WithNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesMin, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func benchmarkSeriesAggregate(b *testing.B, aggFunc crossSeriesAggFunc, numSeries int, fn0, fn1 func() []schema.Point) {
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
	out := make([]schema.Point, 0, len(input[0].Datapoints))
	for i := 0; i < b.N; i++ {
		aggFunc(input, &out)
		out = out[:0]
	}
	b.SetBytes(int64(numSeries * len(input[0].Datapoints) * 12))
}
