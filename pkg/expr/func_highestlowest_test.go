package expr

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/internal/test"
	"github.com/grafana/metrictank/pkg/api/models"
)

func TestHighestAverage(t *testing.T) {
	testHighestLowest(
		"highest(average,1)",
		"average",
		1,
		true,
		[]models.Series{
			getSeriesNamed("a", a),
		},
		[]models.Series{
			getSeriesNamed("a", a),
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
			getSeriesNamed("b", b),
			getSeriesNamed("a", a),
			getSeriesNamed("c", c),
		},
		[]models.Series{
			getSeriesNamed("c", c),
			getSeriesNamed("a", a),
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
			getSeriesNamed("avg4a2b", avg4a2b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("b", b),
			getSeriesNamed("b", b),
		},
		[]models.Series{
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("avg4a2b", avg4a2b),
			getSeriesNamed("b", b),
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
			getSeriesNamed("sumab", sumab),
			getSeriesNamed("b", b),
		},
		[]models.Series{
			getSeriesNamed("sumab", sumab),
			getSeriesNamed("b", b),
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
			getSeriesNamed("avg4a2b", avg4a2b),
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("b", b),
		},
		[]models.Series{
			getSeriesNamed("avg4a2b", avg4a2b),
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
			getSeriesNamed("c", c),
			getSeriesNamed("d", d),
			getSeriesNamed("sumabc", sumabc),
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("a", a),
		},
		[]models.Series{
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("sumabc", sumabc),
			getSeriesNamed("a", a),
			getSeriesNamed("d", d),
			getSeriesNamed("c", c),
		},
		t,
	)
}

func TestHighestExtraLong(t *testing.T) {
	highAvg := []schema.Point{
		{Val: math.MaxFloat64, Ts: 10},
		{Val: math.MaxFloat64, Ts: 20},
		{Val: math.MaxFloat64, Ts: 30},
		{Val: math.MaxFloat64, Ts: 40},
		{Val: math.MaxFloat64, Ts: 50},
		{Val: math.MaxFloat64, Ts: 60},
	}
	series := []models.Series{
		getSeriesNamed("a",
			[]schema.Point{
				{Val: 0, Ts: 10},
				{Val: 0, Ts: 20},
				{Val: 0, Ts: 30},
				{Val: 0, Ts: 40},
				{Val: 0, Ts: 50},
				{Val: 0, Ts: 60},
			}),
		getSeriesNamed("b",
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 1, Ts: 20},
				{Val: 1, Ts: 30},
				{Val: 1, Ts: 40},
				{Val: 1, Ts: 50},
				{Val: 1, Ts: 60},
			}),
		getSeriesNamed("avg4a2b",
			[]schema.Point{
				{Val: 2, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 2, Ts: 30},
				{Val: 2, Ts: 40},
				{Val: 2, Ts: 50},
				{Val: 2, Ts: 60},
			}),
		getSeriesNamed("sum4a2b",
			[]schema.Point{
				{Val: 3, Ts: 10},
				{Val: 3, Ts: 20},
				{Val: 3, Ts: 30},
				{Val: 3, Ts: 40},
				{Val: 3, Ts: 50},
				{Val: 3, Ts: 60},
			}),
		getSeriesNamed("c",
			[]schema.Point{
				{Val: 4, Ts: 10},
				{Val: 4, Ts: 20},
				{Val: 4, Ts: 30},
				{Val: 4, Ts: 40},
				{Val: 4, Ts: 50},
				{Val: 4, Ts: 60},
			}),
		getSeriesNamed("d",
			[]schema.Point{
				{Val: 5, Ts: 10},
				{Val: 5, Ts: 20},
				{Val: 5, Ts: 30},
				{Val: 5, Ts: 40},
				{Val: 5, Ts: 50},
				{Val: 5, Ts: 60},
			}),
		getSeriesNamed("sumab",
			[]schema.Point{
				{Val: 6, Ts: 10},
				{Val: 6, Ts: 20},
				{Val: 6, Ts: 30},
				{Val: 6, Ts: 40},
				{Val: 6, Ts: 50},
				{Val: 6, Ts: 60},
			}),
		getSeriesNamed("sumabc",
			[]schema.Point{
				{Val: 7, Ts: 10},
				{Val: 7, Ts: 20},
				{Val: 7, Ts: 30},
				{Val: 7, Ts: 40},
				{Val: 7, Ts: 50},
				{Val: 7, Ts: 60},
			}),
		getSeriesNamed("sumcd",
			[]schema.Point{
				{Val: 8, Ts: 10},
				{Val: 8, Ts: 20},
				{Val: 8, Ts: 30},
				{Val: 8, Ts: 40},
				{Val: 8, Ts: 50},
				{Val: 8, Ts: 60},
			}),
		getSeriesNamed("avgab",
			[]schema.Point{
				{Val: 9, Ts: 10},
				{Val: 9, Ts: 20},
				{Val: 9, Ts: 30},
				{Val: 9, Ts: 40},
				{Val: 9, Ts: 50},
				{Val: 9, Ts: 60},
			}),
	}
	for i := 0; i < 10; i++ {
		for _, serie := range series {
			serie.Datapoints = getCopy(serie.Datapoints)
			series = append(series, serie)
		}
	}
	series = append(series, getSeriesNamed("highAvg", highAvg))
	rand.Shuffle(len(series), func(i, j int) {
		series[i], series[j] = series[j], series[i]
	})
	fmt.Println(len(series))
	testHighestLowest(
		"highest(average,1)",
		"average",
		1,
		true,
		series,
		[]models.Series{
			getSeriesNamed("highAvg", highAvg),
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
			getSeriesNamed("avg4a2b", avg4a2b),
			getSeriesNamed("sum4a2b", sum4a2b),
			getSeriesNamed("b", b),
		},
		[]models.Series{},
		t,
	)
}

func testHighestLowest(name string, fn string, n int64, highest bool, in []models.Series, out []models.Series, t *testing.T) {
	f := NewHighestLowestConstructor(fn, highest)()
	f.(*FuncHighestLowest).in = NewMock(in)
	f.(*FuncHighestLowest).n = n

	//inputCopy := models.SeriesCopy(in) // to later verify that it is unchanged

	dataMap := initDataMap(in)

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}

	/*
		TODO - does sorting count as modification?
		t.Run("DidNotModifyInput", func(t *testing.T) {
			if err := equalOutput(inputCopy, in, nil, nil); err != nil {
				t.Fatalf("Input was modified, err = %s", err)
			}
		})
	*/

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Point slices in datamap overlap, err = %s", err)
		}
	})
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

func benchmarkHighestLowest(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			QueryPatt: strconv.Itoa(i),
		}
		if i%2 == 0 {
			series.Datapoints, series.Interval = fn0()
		} else {
			series.Datapoints, series.Interval = fn1()
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
