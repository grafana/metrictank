package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

var basic = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 10, Ts: 20},
	{Val: 20, Ts: 30},
}

var basicR = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: float64(10) / 20, Ts: 20},
	{Val: 1, Ts: 30},
}

var nan = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.NaN(), Ts: 20},
	{Val: 20, Ts: 30},
}

var nanR = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: math.NaN() / 20, Ts: 20},
	{Val: 1, Ts: 30},
}

var minMaxSame = []schema.Point{
	{Val: 20, Ts: 10},
	{Val: 20, Ts: 20},
	{Val: 20, Ts: 30},
}

var minMaxSameR = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: 0, Ts: 30},
}

var infinity = []schema.Point{
	{Val: 100, Ts: 10},
	{Val: 1000, Ts: 20},
	{Val: math.Inf(0), Ts: 30},
}

// infinity / infinity is undefined
var infinityR = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: math.NaN(), Ts: 30},
}

func TestMinMaxBasic(t *testing.T) {
	testMinMax("NaN",
		[]models.Series{
			getSeries("targetHere", "queryPattHere", basic),
		},
		[]models.Series{
			getSeries("minMax(targetHere)", "minMax(queryPattHere)", basicR),
		},
		t)
}

func TestMinMaxNaN(t *testing.T) {
	testMinMax("NaN",
		[]models.Series{
			getSeries("targetHere", "queryPattHere", nan),
		},
		[]models.Series{
			getSeries("minMax(targetHere)", "minMax(queryPattHere)", nanR),
		},
		t)
}

func TestMinMaxSame(t *testing.T) {
	testMinMax("SameMinMax",
		[]models.Series{
			getSeries("targetHere", "queryPattHere", minMaxSame),
		},
		[]models.Series{
			getSeries("minMax(targetHere)", "minMax(queryPattHere)", minMaxSameR),
		},
		t)
}

func TestMinMaxInfinity(t *testing.T) {
	testMinMax("Infinity",
		[]models.Series{
			getSeries("targetHere", "queryPattHere", infinity),
		},
		[]models.Series{
			getSeries("minMax(targetHere)", "minMax(queryPattHere)", infinityR),
		},
		t)
}

func TestMinMaxZero(t *testing.T) {
	testMinMax("Zero",
		[]models.Series{
			getSeries("targetHere", "queryPattHere", []schema.Point{}),
		},
		[]models.Series{
			getSeries("minMax(targetHere)", "minMax(queryPattHere)", []schema.Point{}),
		},
		t)
}

func TestMinMaxMultiple(t *testing.T) {
	testMinMax("MultipleSeries",
		[]models.Series{
			getSeries("targetHere", "queryPattHere", basic),
			getSeries("targetHere2", "queryPattHere2", basic),
		}, []models.Series{
			getSeries("minMax(targetHere)", "minMax(queryPattHere)", basicR),
			getSeries("minMax(targetHere2)", "minMax(queryPattHere2)", basicR),
		},
		t)
}

func getNewMinMax(in []models.Series) *FuncMinMax {
	f := NewMinMax()
	ps := f.(*FuncMinMax)
	ps.in = NewMock(in)
	return ps
}

func testMinMax(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := getNewMinMax(in)

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

	dataMap := DataMap(make(map[Req][]models.Series))

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
}

func BenchmarkMinMax10k_1NoNulls(b *testing.B) {
	benchmarkMinMax(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkMinMax10k_10NoNulls(b *testing.B) {
	benchmarkMinMax(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkMinMax10k_100NoNulls(b *testing.B) {
	benchmarkMinMax(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkMinMax10k_1000NoNulls(b *testing.B) {
	benchmarkMinMax(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkMinMax10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkMinMax(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMinMax10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkMinMax(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMinMax10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkMinMax(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMinMax10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkMinMax(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMinMax10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkMinMax(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMinMax10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkMinMax(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMinMax10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkMinMax(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkMinMax10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkMinMax(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkMinMax(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewMinMax()
		f.(*FuncMinMax).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
