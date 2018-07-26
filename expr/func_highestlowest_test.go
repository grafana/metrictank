package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"gopkg.in/raintank/schema.v1"
)

func TestHighestAverage(t *testing.T) {
	testHighestLowest(
		"highest(average,1)",
		"average",
		1,
		true,
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
				QueryPatt:  "a",
				Datapoints: getCopy(a),
			},
		},
		t,
	)
}

func TestLowestAverage(t *testing.T) {
	testHighestLowest(
		"lowest(average,2)",
		"average",
		2,
		false,
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "c",
				Datapoints: getCopy(c),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "a",
				Datapoints: getCopy(a),
			},
		},
		t,
	)
}

func TestHighestCurrent(t *testing.T) {
	testHighestLowest(
		"highest(current,3)",
		"current",
		3,
		true,
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "avg4a2b",
				Datapoints: getCopy(avg4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "sum4a2b",
				Datapoints: getCopy(sum4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "sum4a2b",
				Datapoints: getCopy(sum4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "avg4a2b",
				Datapoints: getCopy(avg4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
		},
		t,
	)
}

func TestLowestCurrent(t *testing.T) {
	testHighestLowest(
		"highest(current,4)",
		"current",
		4,
		true,
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "sumab",
				Datapoints: getCopy(sumab),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "sumab",
				Datapoints: getCopy(sumab),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
		},
		t,
	)
}

func TestHighestMax(t *testing.T) {
	testHighestLowest(
		"highest(max,1)",
		"max",
		1,
		true,
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "avg4a2b",
				Datapoints: getCopy(avg4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "sum4a2b",
				Datapoints: getCopy(sum4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "avg4a2b",
				Datapoints: getCopy(avg4a2b),
			},
		},
		t,
	)
}

func TestHighestLong(t *testing.T) {
	testHighestLowest(
		"highest(current,5)",
		"current",
		5,
		true,
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "d",
				Datapoints: getCopy(d),
			},
			{
				Interval:   10,
				QueryPatt:  "sumabc",
				Datapoints: getCopy(sumabc),
			},
			{
				Interval:   10,
				QueryPatt:  "sum4a2b",
				Datapoints: getCopy(sum4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "a",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "sum4a2b",
				Datapoints: getCopy(sum4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "sumabc",
				Datapoints: getCopy(sumabc),
			},
			{
				Interval:   10,
				QueryPatt:  "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "d",
				Datapoints: getCopy(d),
			},
			{
				Interval:   10,
				QueryPatt:  "c",
				Datapoints: getCopy(c),
			},
		},
		t,
	)
}

func TestHighestNone(t *testing.T) {
	testHighestLowest(
		"highest(average,0)",
		"average",
		0,
		true,
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "avg4a2b",
				Datapoints: getCopy(avg4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "sum4a2b",
				Datapoints: getCopy(sum4a2b),
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Datapoints: getCopy(b),
			},
		},
		[]models.Series{},
		t,
	)
}

func testHighestLowest(name string, fn string, n int64, highest bool, in []models.Series, out []models.Series, t *testing.T) {
	f := NewHighestLowestConstructor(fn, highest)()
	f.(*FuncHighestLowest).in = NewMock(in)
	f.(*FuncHighestLowest).n = n
	gots, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(gots) != len(out) {
		t.Fatalf("case %q: len output expected %d, got %d", name, len(out), len(gots))
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
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(exp.Datapoints[j].Val)
			if (bothNaN || p.Val == exp.Datapoints[j].Val) && p.Ts == exp.Datapoints[j].Ts {
				continue
			}
			t.Fatalf("case %q: output point %d - expected %v got %v", name, j, exp.Datapoints[j], p)
		}
	}
}

func BenchmarkHighestLowest10k_1NoNulls(b *testing.B) {
	benchmarkHighestLowest(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkHighestLowest10k_10NoNulls(b *testing.B) {
	benchmarkHighestLowest(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkHighestLowest10k_100NoNulls(b *testing.B) {
	benchmarkHighestLowest(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkHighestLowest10k_1000NoNulls(b *testing.B) {
	benchmarkHighestLowest(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkHighestLowest10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkHighestLowest(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkHighestLowest10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkHighestLowest(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkHighestLowest10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkHighestLowest(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkHighestLowest10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkHighestLowest(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkHighestLowest10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkHighestLowest(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkHighestLowest10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkHighestLowest(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkHighestLowest10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkHighestLowest(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkHighestLowest10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkHighestLowest(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkHighestLowest(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f := NewHighestLowestConstructor("average", true)()
		f.(*FuncHighestLowest).in = NewMock(input)
		f.(*FuncHighestLowest).n = 5
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
