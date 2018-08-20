package expr

import (
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
)

func TestFilterSeriesEqual(t *testing.T) {

	testFilterSeries(
		"abcd_equal",
		"max",
		"=",
		1234567890,
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
		},
		t,
	)
}

func TestFilterSeriesNotEqual(t *testing.T) {

	testFilterSeries(
		"abcd_notequal",
		"max",
		"!=",
		1234567890,
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		t,
	)
}

func TestFilterSeriesLessThan(t *testing.T) {

	testFilterSeries(
		"abcd_lessthan",
		"max",
		"<",
		1234567890,
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		t,
	)
}

func TestFilterSeriesLessThanOrEqualTo(t *testing.T) {

	testFilterSeries(
		"abcd_lessorequal",
		"max",
		"<=",
		250,
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		t,
	)
}

func TestFilterSeriesMoreThan(t *testing.T) {

	testFilterSeries(
		"abcd_more",
		"max",
		">",
		250,
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
		},
		t,
	)
}

func TestFilterSeriesMoreThanOrEqual(t *testing.T) {

	testFilterSeries(
		"abcd_moreorequal",
		"max",
		">=",
		250,
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		t,
	)
}

func testFilterSeries(name string, fn string, operator string, threshold float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewFilterSeries()
	f.(*FuncFilterSeries).in = NewMock(in)
	f.(*FuncFilterSeries).fn = fn
	f.(*FuncFilterSeries).operator = operator
	f.(*FuncFilterSeries).threshold = threshold
	gots, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q (%s, %s, %f): err should be nil. got %q", name, fn, operator, threshold, err)
	}
	if len(gots) != len(out) {
		t.Fatalf("case %q (%s, %s, %f): isNonNull len output expected %d, got %d", name, fn, operator, threshold, len(out), len(gots))
	}
	for i, g := range gots {
		exp := out[i]
		if g.Target != exp.Target {
			t.Fatalf("case %q (%s, %s, %f): expected target %q, got %q", name, fn, operator, threshold, exp.Target, g.Target)
		}
		if len(g.Datapoints) != len(exp.Datapoints) {
			t.Fatalf("case %q (%s, %s, %f) len output expected %d, got %d", name, fn, operator, threshold, len(exp.Datapoints), len(g.Datapoints))
		}
		for j, p := range g.Datapoints {
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(exp.Datapoints[j].Val)
			if (bothNaN || p.Val == exp.Datapoints[j].Val) && p.Ts == exp.Datapoints[j].Ts {
				continue
			}
			t.Fatalf("case %q (%s, %s, %f): output point %d - expected %v got %v", name, fn, operator, threshold, j, exp.Datapoints[j], p)
		}
	}
}

func BenchmarkFilterSeries10k_1NoNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkFilterSeries10k_10NoNulls(b *testing.B) {
	benchmarkFilterSeries(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkFilterSeries10k_100NoNulls(b *testing.B) {
	benchmarkFilterSeries(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkFilterSeries10k_1000NoNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkFilterSeries10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkFilterSeries10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkFilterSeries(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewFilterSeries()
		f.(*FuncFilterSeries).in = NewMock(input)
		f.(*FuncFilterSeries).fn = "sum"
		f.(*FuncFilterSeries).operator = ">"
		f.(*FuncFilterSeries).threshold = rand.Float64()
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
