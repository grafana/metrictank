package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/schema"
	"github.com/grafana/metrictank/pkg/test"
)

var datapoints = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.NaN(), Ts: 20},
	{Val: -10, Ts: 30},
	{Val: 5.5, Ts: 40},
	{Val: -math.MaxFloat64, Ts: 50},
	{Val: -1234567890, Ts: 60},
	{Val: math.MaxFloat64, Ts: 70},
}

var datapointsInvert = []schema.Point{
	{Val: math.NaN(), Ts: 10},
	{Val: math.NaN(), Ts: 20},
	{Val: math.Pow(-10, -1), Ts: 30},
	{Val: math.Pow(5.5, -1), Ts: 40},
	{Val: math.Pow(-math.MaxFloat64, -1), Ts: 50},
	{Val: math.Pow(-1234567890, -1), Ts: 60},
	{Val: math.Pow(math.MaxFloat64, -1), Ts: 70},
}

var datapointsInterval20 = []schema.Point{
	{Val: -5.5, Ts: 20},
	{Val: -math.MaxFloat64, Ts: 40},
	{Val: 1234567890, Ts: 60},
}

var datapointsInterval20Invert = []schema.Point{
	{Val: math.Pow(-5.5, -1), Ts: 20},
	{Val: math.Pow(-math.MaxFloat64, -1), Ts: 40},
	{Val: math.Pow(1234567890, -1), Ts: 60},
}

func TestInvertBasic(t *testing.T) {
	in := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "queryPattHere",
			Target:     "targetHere",
			Datapoints: getCopy(datapoints),
			QueryFrom:  8,
			QueryTo:    75,
		},
	}

	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "invert(queryPattHere)",
			Target:     "invert(targetHere)",
			Datapoints: getCopy(datapointsInvert),
			QueryFrom:  8,
			QueryTo:    75,
		},
	}

	testInvertSeries("basic", in, out, t)
}

func TestInvertMultiple(t *testing.T) {
	in := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "queryPattHere",
			Target:     "targetHere",
			Datapoints: getCopy(datapoints),
			QueryFrom:  8,
			QueryTo:    75,
		},
		{
			Interval:   20,
			QueryPatt:  "queryPattHere2",
			Target:     "targetHere2",
			Datapoints: getCopy(datapointsInterval20),
			QueryFrom:  8,
			QueryTo:    75,
		},
	}

	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "invert(queryPattHere)",
			Target:     "invert(targetHere)",
			Datapoints: getCopy(datapointsInvert),
			QueryFrom:  8,
			QueryTo:    75,
		},
		{
			Interval:   20,
			QueryPatt:  "invert(queryPattHere2)",
			Target:     "invert(targetHere2)",
			Datapoints: getCopy(datapointsInterval20Invert),
			QueryFrom:  8,
			QueryTo:    75,
		},
	}

	testInvertSeries("multiple", in, out, t)
}

func getNewInvert(in []models.Series) *FuncInvert {
	f := NewInvert()
	ps := f.(*FuncInvert)
	ps.in = NewMock(in)
	return ps
}

func testInvertSeries(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := getNewInvert(in)

	inputCopy := models.SeriesCopy(in) // to later verify that it is unchanged

	dataMap := initDataMapMultiple([][]models.Series{in})
	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Case %s: Input was modified, err = %s", name, err)
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", name, err)
		}
	})
	t.Run("OutputIsCanonical", func(t *testing.T) {
		for i, s := range got {
			if !s.IsCanonical() {
				t.Fatalf("Case %s: output series %d is not canonical: %v", name, i, s)
			}
		}
	})
}

func BenchmarkInvert10k_1NoNulls(b *testing.B) {
	benchmarkInvert(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkInvert10k_10NoNulls(b *testing.B) {
	benchmarkInvert(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkInvert10k_100NoNulls(b *testing.B) {
	benchmarkInvert(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkInvert10k_1000NoNulls(b *testing.B) {
	benchmarkInvert(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkInvert10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkInvert10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkInvert(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkInvert(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
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
	for i := 0; i < b.N; i++ {
		f := NewInvert()
		f.(*FuncInvert).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
