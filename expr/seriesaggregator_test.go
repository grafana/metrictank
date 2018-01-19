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
	zeroOutput := getCopy(a)
	for i := range zeroOutput {
		if !math.IsNaN(zeroOutput[i].Val) {
			zeroOutput[i].Val = 0
		}
	}
	testSeriesAggregate("identity", "average", input, getCopy(a), t)
	testSeriesAggregate("identity", "sum", input, getCopy(a), t)
	testSeriesAggregate("identity", "max", input, getCopy(a), t)
	testSeriesAggregate("identity", "min", input, getCopy(a), t)
	testSeriesAggregate("identity", "multiply", input, getCopy(a), t)
	testSeriesAggregate("identity", "median", input, getCopy(a), t)
	testSeriesAggregate("identity", "diff", input, getCopy(a), t)
	testSeriesAggregate("identity", "stddev", input, zeroOutput, t)
	testSeriesAggregate("identity", "rangeOf", input, zeroOutput, t)
	testSeriesAggregate("identity", "range", input, zeroOutput, t)
}

func TestSeriesAggregate2series(t *testing.T) {
	input := []models.Series{
		{
			QueryPatt:  "foo.*",
			Datapoints: getCopy(a),
		},
		{
			QueryPatt:  "foo.*",
			Datapoints: getCopy(b),
		},
	}

	testSeriesAggregate("2Series", "average", input, getCopy(avgab), t)
	testSeriesAggregate("2Series", "sum", input, getCopy(sumab), t)
	testSeriesAggregate("2Series", "max", input, getCopy(maxab), t)
	testSeriesAggregate("2Series", "min", input, getCopy(minab), t)
	testSeriesAggregate("2Series", "multiply", input, getCopy(multab), t)
	testSeriesAggregate("2Series", "median", input, getCopy(medianab), t)
	testSeriesAggregate("2Series", "diff", input, getCopy(diffab), t)
	testSeriesAggregate("2Series", "stddev", input, getCopy(stddevab), t)
	testSeriesAggregate("2Series", "range", input, getCopy(rangeab), t)
}

func TestSeriesAggregate3series(t *testing.T) {
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

	testSeriesAggregate("3Series", "average", input, getCopy(avgabc), t)
	testSeriesAggregate("3Series", "sum", input, getCopy(sumabc), t)
	testSeriesAggregate("3Series", "max", input, getCopy(maxabc), t)
	testSeriesAggregate("3Series", "min", input, getCopy(minabc), t)
	testSeriesAggregate("3Series", "multiply", input, getCopy(multabc), t)
	testSeriesAggregate("3Series", "median", input, getCopy(medianabc), t)
	testSeriesAggregate("3Series", "diff", input, getCopy(diffabc), t)
	testSeriesAggregate("3Series", "stddev", input, getCopy(stddevabc), t)
	testSeriesAggregate("3Series", "range", input, getCopy(rangeabc), t)
}

func testSeriesAggregate(name, agg string, in []models.Series, out []schema.Point, t *testing.T) {
	f := getCrossSeriesAggFunc(agg)

	got := make([]schema.Point, 0, len(out))
	f(in, &got)

	if len(got) != len(out) {
		t.Fatalf("case %q (%q): len output expected %d, got %d", name, agg, len(out), len(got))
	}

	// Use EPSILON to avoid floating-point precision errors
	EPSILON := math.Nextafter(1, 2) - 1

	for j, p := range got {
		bothNaN := math.IsNaN(p.Val) && math.IsNaN(out[j].Val)
		if (bothNaN || p.Val == out[j].Val || math.Abs(p.Val-out[j].Val) < EPSILON) && p.Ts == out[j].Ts {
			continue
		}
		t.Fatalf("case %q (%q): output point %d - expected %v got %v", name, agg, j, out[j], p)
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

func BenchmarkSeriesAggregateMultiply10k_100NoNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesMultiply, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSeriesAggregateMultiply10k_100WithNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesMultiply, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkSeriesAggregateMedian10k_100NoNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesMedian, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSeriesAggregateMedian10k_100WithNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesMedian, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkSeriesAggregateDiff10k_100NoNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesDiff, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSeriesAggregateDiff10k_100WithNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesDiff, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkSeriesAggregateStddev10k_100NoNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesStddev, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSeriesAggregateStddev10k_100WithNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesStddev, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkSeriesAggregateRange10k_100NoNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesRange, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSeriesAggregateRange10k_100WithNulls(b *testing.B) {
	benchmarkSeriesAggregate(b, crossSeriesRange, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
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
