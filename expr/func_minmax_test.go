package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func getNewMinMax(in []models.Series) *FuncMinMax {
	f := NewMinMax()
	ps := f.(*FuncMinMax)
	ps.in = NewMock(in)
	return ps
}

const interval = 10

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
	f := getNewMinMax(
		[]models.Series{
			{
				Interval:   interval,
				QueryPatt:  "queryPattHere",
				Target:     "targetHere",
				Datapoints: getCopy(basic),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   interval,
			QueryPatt:  "minMax(queryPattHere)",
			Target:     "minMax(targetHere)",
			Datapoints: getCopy(basicR),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestMinMaxNaN(t *testing.T) {
	f := getNewMinMax(
		[]models.Series{
			{
				Interval:   interval,
				QueryPatt:  "queryPattHere",
				Target:     "targetHere",
				Datapoints: getCopy(nan),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   interval,
			QueryPatt:  "minMax(queryPattHere)",
			Target:     "minMax(targetHere)",
			Datapoints: getCopy(nanR),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestMinMaxSame(t *testing.T) {
	f := getNewMinMax(
		[]models.Series{
			{
				Interval:   interval,
				QueryPatt:  "queryPattHere",
				Target:     "targetHere",
				Datapoints: getCopy(minMaxSame),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   interval,
			QueryPatt:  "minMax(queryPattHere)",
			Target:     "minMax(targetHere)",
			Datapoints: getCopy(minMaxSameR),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestMinMaxInputUnchanged(t *testing.T) {
	in := []models.Series{
		{
			Interval:   interval,
			QueryPatt:  "queryPattHere",
			Target:     "targetHere",
			Datapoints: []schema.Point{},
		},
	}

	// store copy of the original input
	inCopy := make([]models.Series, len(in))
	copy(inCopy, in)

	f := getNewMinMax(in)
	_, err := f.Exec(make(map[Req][]models.Series))

	// make sure input hasn't changed after call to minMax
	if err := equalOutput(in, inCopy, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestMinMaxInfinity(t *testing.T) {
	f := getNewMinMax(
		[]models.Series{
			{
				Interval:   interval,
				QueryPatt:  "queryPattHere",
				Target:     "targetHere",
				Datapoints: getCopy(infinity),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   interval,
			QueryPatt:  "minMax(queryPattHere)",
			Target:     "minMax(targetHere)",
			Datapoints: getCopy(infinityR),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestMinMaxZero(t *testing.T) {
	f := getNewMinMax(
		[]models.Series{
			{
				Interval:   interval,
				QueryPatt:  "queryPattHere",
				Target:     "targetHere",
				Datapoints: []schema.Point{},
			},
		},
	)
	out := []models.Series{
		{
			Interval:   interval,
			QueryPatt:  "minMax(queryPattHere)",
			Target:     "minMax(targetHere)",
			Datapoints: []schema.Point{},
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestMinMaxMultiple(t *testing.T) {
	f := getNewMinMax(
		[]models.Series{
			{
				Interval:   interval,
				QueryPatt:  "queryPattHere",
				Target:     "targetHere",
				Datapoints: getCopy(basic),
			},
			{
				Interval:   20,
				QueryPatt:  "queryPattHere2",
				Target:     "targetHere2",
				Datapoints: getCopy(basic),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   interval,
			QueryPatt:  "minMax(queryPattHere)",
			Target:     "minMax(targetHere)",
			Datapoints: getCopy(basicR),
		},
		{
			Interval:   20,
			QueryPatt:  "minMax(queryPattHere2)",
			Target:     "minMax(targetHere2)",
			Datapoints: getCopy(basicR),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
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
