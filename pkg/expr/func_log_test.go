package expr

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/test"
)

func TestLogNoInput(t *testing.T) {
	testLog("no_input", 10, []models.Series{}, []models.Series{}, t)
}

func TestLogSingle(t *testing.T) {
	outBased10 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 1.5185139398778875, Ts: 20},
		{Val: 2.298853076409707, Ts: 30},
		{Val: 1.462397997898956, Ts: 40},
		{Val: 1.9030899869919435, Ts: 50},
		{Val: 2.3979400086720375, Ts: 60},
	}

	outBasedE := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 3.4965075614664802, Ts: 20},
		{Val: 5.293304824724492, Ts: 30},
		{Val: 3.367295829986474, Ts: 40},
		{Val: 4.382026634673881, Ts: 50},
		{Val: 5.521460917862246, Ts: 60},
	}

	testLog(
		"single",
		10,
		[]models.Series{
			getSeriesNamed("d", d),
		},
		[]models.Series{
			getSeriesNamed("log(d,10)", outBased10),
		},
		t,
	)

	testLog(
		"single",
		math.E,
		[]models.Series{
			getSeriesNamed("d2", d),
		},
		[]models.Series{
			getSeriesNamed(fmt.Sprintf("log(d2,%g)", math.E), outBasedE),
		},
		t,
	)
}

func TestLogMultiple(t *testing.T) {
	outBased10 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 1.5185139398778875, Ts: 20},
		{Val: 2.298853076409707, Ts: 30},
		{Val: 1.462397997898956, Ts: 40},
		{Val: 1.9030899869919435, Ts: 50},
		{Val: 2.3979400086720375, Ts: 60},
	}

	testLog(
		"multiple",
		10,
		[]models.Series{
			getSeriesNamed("d", d),
			getSeriesNamed("d2", d),
		},
		[]models.Series{
			getSeriesNamed("log(d,10)", outBased10),
			getSeriesNamed("log(d2,10)", outBased10),
		},
		t,
	)
}

func TestLogSpecialValues(t *testing.T) {
	specialValues := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 1, Ts: 20},
		{Val: -1.8, Ts: 30},
		{Val: math.MaxFloat64, Ts: 40},
		{Val: math.Inf(1), Ts: 50},
		{Val: math.Inf(-1), Ts: 60},
		{Val: 1024, Ts: 70},
	}
	out := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 0, Ts: 20},
		{Val: math.NaN(), Ts: 30},
		{Val: -1024, Ts: 40},
		{Val: math.Inf(-1), Ts: 50},
		{Val: math.NaN(), Ts: 60},
		{Val: -10, Ts: 70},
	}

	testLog(
		"specialValues",
		0.5,
		[]models.Series{
			getSeriesNamed("specialValues", specialValues),
		},
		[]models.Series{
			getSeriesNamed("log(specialValues,0.5)", out),
		},
		t,
	)
}

func testLog(name string, base float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewLog()
	f.(*FuncLog).in = NewMock(in)
	f.(*FuncLog).base = base

	inputCopy := models.SeriesCopy(in) // to later verify that it is unchanged

	dataMap := initDataMap(in)

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

func BenchmarkLog10k_1NoNulls(b *testing.B) {
	benchmarkLog(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkLog10k_10NoNulls(b *testing.B) {
	benchmarkLog(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkLog10k_100NoNulls(b *testing.B) {
	benchmarkLog(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkLog10k_1000NoNulls(b *testing.B) {
	benchmarkLog(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkLog10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkLog(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLog10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkLog(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLog10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkLog(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLog10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkLog(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLog10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkLog(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLog10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkLog(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLog10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkLog(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLog10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkLog(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkLog(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
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
		f := NewScale()
		f.(*FuncScale).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
