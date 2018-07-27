package expr

import (
	"math"
	"sort"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"gopkg.in/raintank/schema.v1"
)

func TestAsPercentSingleNoArg(t *testing.T) {

	out := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 100, Ts: 60},
	}
	testAsPercent(
		"single-noarg",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "a;tag=something;tag2=anything",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
				Target:     "asPercent(a;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
				Datapoints: out,
			},
		},
		t,
		math.NaN(),
		nil,
		nil,
		"None",
	)
}

func TestAsPercentMultipleNoArg(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5 / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 100, Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 100, Ts: 20},
		{Val: (math.MaxFloat64 - 20) / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: 100, Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	testAsPercent(
		"multi-noarg",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "a;tag=something;tag2=anything",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "b;tag=something;tag2=anything",
				Datapoints: getCopy(b),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
				Target:     "asPercent(a;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
				Datapoints: out1,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
				Target:     "asPercent(b;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
				Datapoints: out2,
			},
		},
		t,
		math.NaN(),
		nil,
		nil,
		"None",
	)
}

func TestAsPercentTotalFloat(t *testing.T) {
	total := 123.456
	out := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5 / total * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 1234567890 / total * 100, Ts: 60},
	}
	testAsPercent(
		"single-noarg",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(a;tag=something;tag2=anything)",
				Target:     "a;tag=something;tag2=anything",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(a;tag=something;tag2=anything),123.456)",
				Target:     "asPercent(a;tag=something;tag2=anything,123.456)",
				Datapoints: out,
			},
		},
		t,
		total,
		nil,
		nil,
		"123.456",
	)
}

func TestAsPercentTotalSerie(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: math.Inf(0), Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: 199 / 5.5 * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: float64(250) / 1234567890 * 100, Ts: 60},
	}
	testAsPercent(
		"multi-serietotal",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "b;tag=something;tag2=anything",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "d;tag=something;tag2=anything",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=some;tag2=totalSerie))",
				Target:     "asPercent(b;tag=something;tag2=anything,func(tag=some;tag2=totalSerie))",
				Datapoints: out1,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=some;tag2=totalSerie))",
				Target:     "asPercent(d;tag=something;tag2=anything,func(tag=some;tag2=totalSerie))",
				Datapoints: out2,
			},
		},
		t,
		math.NaN(),
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=some;tag2=totalSerie)",
				Target:     "a;tag=some;tag2=totalSerie",
				Datapoints: getCopy(a),
			},
		},
		nil,
		"serie",
	)
}

func TestAsPercentTotalSeries(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: math.Inf(0), Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: float64(199) * 100, Ts: 30},
		{Val: float64(29) / 2 * 100, Ts: 40},
		{Val: float64(80) / 3.0 * 100, Ts: 50},
		{Val: float64(250) / 4 * 100, Ts: 60},
	}
	testAsPercent(
		"multi-seriestotal",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "b;tag=something;tag2=anything",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "d;tag=something;tag2=anything",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=some;tag2=totalSerie))",
				Target:     "asPercent(b;tag=something;tag2=anything,b;tag=some;tag2=totalSerie)",
				Datapoints: out1,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=some;tag2=totalSerie))",
				Target:     "asPercent(d;tag=something;tag2=anything,c;tag=some;tag2=totalSerie)",
				Datapoints: out2,
			},
		},
		t,
		math.NaN(),
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=some;tag2=totalSerie)",
				Target:     "c;tag=some;tag2=totalSerie",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=some;tag2=totalSerie)",
				Target:     "b;tag=some;tag2=totalSerie",
				Datapoints: getCopy(a),
			},
		},
		nil,
		"series",
	)
}

func TestAsPercentNoArgNodes(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5 / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 100, Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 100, Ts: 20},
		{Val: (math.MaxFloat64 - 20) / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: 100, Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	out3 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: 100, Ts: 30},
		{Val: 100, Ts: 40},
		{Val: 100, Ts: 50},
		{Val: 100, Ts: 60},
	}
	testAsPercent(
		"multi-seriebynode",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "this.that.a;tag=something;tag2=anything",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "this.that.b;tag=something;tag2=anything",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "this.this.c;tag=something;tag2=anything",
				Datapoints: getCopy(c),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
				Target:     "asPercent(this.that.a;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
				Datapoints: out1,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
				Target:     "asPercent(this.that.b;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
				Datapoints: out2,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=something;tag2=anything))",
				Target:     "asPercent(this.this.c;tag=something;tag2=anything,this.this.c;tag=something;tag2=anything)",
				Datapoints: out3,
			},
		},
		t,
		math.NaN(),
		nil,
		[]expr{{etype: etFloat, float: 0}, {etype: etInt, int: 1}},
		"None",
	)
}

func TestAsPercentNoArgTagNodes(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5 / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 100, Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 100, Ts: 20},
		{Val: (math.MaxFloat64 - 20) / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: 100, Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	out3 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: 100, Ts: 30},
		{Val: 100, Ts: 40},
		{Val: 100, Ts: 50},
		{Val: 100, Ts: 60},
	}
	testAsPercent(
		"multi-seriebynode",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=something1;tag2=anything)",
				Target:     "this.that.a;tag=something1;tag2=anything",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something1;tag2=anything)",
				Target:     "this.those.b;tag=something1;tag2=anything",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something2;tag2=anything)",
				Target:     "this.this.c;tag=something2;tag2=anything",
				Datapoints: getCopy(c),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something1;tag2=anything),sumSeries(func(tag=something1;tag2=anything)))",
				Target:     "asPercent(this.that.a;tag=something1;tag2=anything,sumSeries(func(tag=something1;tag2=anything)))",
				Datapoints: out1,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something1;tag2=anything),sumSeries(func(tag=something1;tag2=anything)))",
				Target:     "asPercent(this.those.b;tag=something1;tag2=anything,sumSeries(func(tag=something1;tag2=anything)))",
				Datapoints: out2,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something2;tag2=anything),func(tag=something2;tag2=anything))",
				Target:     "asPercent(this.this.c;tag=something2;tag2=anything,this.this.c;tag=something2;tag2=anything)",
				Datapoints: out3,
			},
		},
		t,
		math.NaN(),
		nil,
		[]expr{{etype: etString, str: "tag"}},
		"None",
	)
}

func TestAsPercentSeriesByNodes(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5 / (math.MaxFloat64) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 1234567890.0 / 1234568148 * 100, Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 100, Ts: 20},
		{Val: 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: 1234567890.0 / 1234567976 * 100, Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	allNaN := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: math.NaN(), Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	testAsPercent(
		"multi-seriebynodeandseries",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "this.that.a;tag=something;tag2=anything",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "this.that.b;tag=something;tag2=anything",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=anything)",
				Target:     "this.this.c;tag=something;tag2=anything",
				Datapoints: getCopy(c),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=totalSerie)))",
				Target:     "asPercent(this.that.a;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=totalSerie)))",
				Datapoints: out1,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=totalSerie)))",
				Target:     "asPercent(this.that.b;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=totalSerie)))",
				Datapoints: out2,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(func(tag=something;tag2=anything),MISSING)",
				Target:     "asPercent(this.this.c;tag=something;tag2=anything,MISSING)",
				Datapoints: allNaN,
			},
			{
				Interval:   10,
				QueryPatt:  "asPercent(MISSING,func(tag=something;tag2=totalSerie))",
				Target:     "asPercent(MISSING,this.those.ab;tag=something;tag2=totalSerie)",
				Datapoints: allNaN,
			},
		},
		t,
		math.NaN(),
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=totalSerie)",
				Target:     "this.those.ab;tag=something;tag2=totalSerie",
				Datapoints: getCopy(sumab),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=totalSerie)",
				Target:     "this.that.abc;tag=something;tag2=totalSerie",
				Datapoints: getCopy(sumabc),
			},
			{
				Interval:   10,
				QueryPatt:  "func(tag=something;tag2=totalSerie)",
				Target:     "this.that.cd;tag=something;tag2=totalSerie",
				Datapoints: getCopy(sumcd),
			},
		},
		[]expr{{etype: etFloat, float: 0}, {etype: etInt, int: 1}},
		"series",
	)
}

func testAsPercent(name string, in []models.Series, out []models.Series, t *testing.T, totalFloat float64, totalSeries []models.Series, nodes []expr, total string) {
	f := NewAsPercent()
	f.(*FuncAsPercent).in = NewMock(in)
	if totalSeries != nil {
		f.(*FuncAsPercent).totalSeries = NewMock(totalSeries)
	}
	f.(*FuncAsPercent).totalFloat = totalFloat
	f.(*FuncAsPercent).nodes = nodes

	originalSeries := make([]models.Series, len(in))
	for i, serie := range in {
		originalSeries[i].Interval = serie.Interval
		originalSeries[i].QueryPatt = serie.QueryPatt
		originalSeries[i].Target = serie.Target
		originalSeries[i].Datapoints = getCopy(serie.Datapoints)
	}
	gots, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q (%q, %v): err should be nil. got %q", name, total, nodes, err)
	}
	sort.Slice(gots, func(i, j int) bool { return gots[i].Target < gots[j].Target })
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	if len(gots) != len(out) {
		t.Fatalf("case %q (%q, %v): asPercent len output expected %d, got %d", name, total, nodes, len(out), len(gots))
	}
	for i, g := range gots {
		exp := out[i]
		if g.Target != exp.Target {
			t.Fatalf("case %q (%q, %v): expected target %q, got %q", name, total, nodes, exp.Target, g.Target)
		}
		if g.QueryPatt != exp.QueryPatt {
			t.Fatalf("case %q (%q, %v),: expected querypatt %q, got %q", name, total, nodes, exp.QueryPatt, g.QueryPatt)
		}
		if len(g.Datapoints) != len(exp.Datapoints) {
			t.Fatalf("case %q (%q, %v): len output expected %d, got %d", name, total, nodes, len(exp.Datapoints), len(g.Datapoints))
		}
		for j, p := range exp.Datapoints {
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(g.Datapoints[j].Val)
			if (bothNaN || p.Val == g.Datapoints[j].Val) && p.Ts == g.Datapoints[j].Ts {
				continue
			}

			t.Fatalf("case %q (%q, %v): output point %d - expected %v got %v", name, total, nodes, j, p, g.Datapoints[j])
		}
	}

	// Test if original series was modified
	for i, orig := range originalSeries {
		inSerie := in[i]
		if orig.Target != inSerie.Target {
			t.Fatalf("case %q (%q, %v): put in target %q, ended with %q", name, total, nodes, orig.Target, inSerie.Target)
		}
		if orig.QueryPatt != inSerie.QueryPatt {
			t.Fatalf("case %q (%q, %v),: put in querypatt %q, ended with %q", name, total, nodes, orig.Target, inSerie.Target)
		}
		if len(orig.Datapoints) != len(inSerie.Datapoints) {
			t.Fatalf("case %q (%q, %v): put in len %d, ended with %d", name, total, nodes, len(inSerie.Datapoints), len(orig.Datapoints))
		}
		for j, p := range inSerie.Datapoints {
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(orig.Datapoints[j].Val)
			if (bothNaN || p.Val == orig.Datapoints[j].Val) && p.Ts == orig.Datapoints[j].Ts {
				continue
			}

			t.Fatalf("case %q (%q, %v): output point %d - put in %v ended with %v", name, total, nodes, j, p, orig.Datapoints[j])
		}
	}
}

func BenchmarkAsPercent10k_1NoNulls(b *testing.B) {
	benchmarkAsPercent(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAsPercent10k_10NoNulls(b *testing.B) {
	benchmarkAsPercent(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAsPercent10k_100NoNulls(b *testing.B) {
	benchmarkAsPercent(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAsPercent10k_1000NoNulls(b *testing.B) {
	benchmarkAsPercent(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkAsPercent10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkAsPercent10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkAsPercent(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewAsPercent()
		f.(*FuncAsPercent).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
