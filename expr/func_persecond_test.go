package expr

import (
	"math"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

var aPerSecond = []schema.Point{
	{Val: math.NaN(), Ts: 10}, // nan to 0
	{Val: 0, Ts: 20},          // 0 to 0
	{Val: 0.55, Ts: 30},       // 0 to 5.5
	{Val: math.NaN(), Ts: 40}, // 5.5 to nan
	{Val: math.NaN(), Ts: 50}, // nan to nan
	{Val: math.NaN(), Ts: 60}, // nan to 1234567890
}

var bPerSecond = []schema.Point{
	{Val: math.NaN(), Ts: 10},           // nan to 0
	{Val: math.MaxFloat64 / 10, Ts: 20}, // 0 to maxFloat
	{Val: 0, Ts: 30},                    // maxFloat to maxFloat -20. really NaN but floating point limitation -> 0
	{Val: math.NaN(), Ts: 40},           // maxFloat -20 to nan
	{Val: math.NaN(), Ts: 50},           // nan to 1234567890
	{Val: math.NaN(), Ts: 60},           // 1234567890 to nan
}

var cPerSecond = []schema.Point{
	{Val: math.NaN(), Ts: 10}, // nan to 0
	{Val: 0, Ts: 20},          // 0 to 0
	{Val: 0.1, Ts: 30},        // 0 to 1
	{Val: 0.1, Ts: 40},        // 1 to 2
	{Val: 0.1, Ts: 50},        // 2 to 3
	{Val: 0.1, Ts: 60},        // 3 to 4
}

var dPerSecondMax255 = []schema.Point{
	{Val: math.NaN(), Ts: 10},               // nan to 0
	{Val: 3.3, Ts: 20},                      // 0 to 33
	{Val: float64(199-33) / 10, Ts: 30},     // 33 to 199
	{Val: float64(29-199+256) / 10, Ts: 40}, // 199 to 29, overflowed after 255
	{Val: float64(80-29) / 10, Ts: 50},      // 29 to 80
	{Val: float64(250-80) / 10, Ts: 60},     // 80 to 250
}

func TestPerSecondSingle(t *testing.T) {
	testPerSecond(
		"identity",
		[][]models.Series{
			{
				getSeriesNamed("a", a),
			},
		},
		[]models.Series{
			getSeriesNamed("perSecond(a)", aPerSecond),
		},
		0,
		t,
	)
}

func TestPerSecondSingleMaxValue(t *testing.T) {
	testPerSecond(
		"identity-counter8bit",
		[][]models.Series{
			{
				getSeriesNamed("counter8bit", d),
			},
		},
		[]models.Series{
			getSeriesNamed("perSecond(counter8bit)", dPerSecondMax255),
		},
		255,
		t,
	)
}

func TestPerSecondMulti(t *testing.T) {
	testPerSecond(
		"multiple-series",
		[][]models.Series{
			{
				getSeriesNamed("a", a),
				getSeriesNamed("b.*", b),
			},
		},
		[]models.Series{
			getSeriesNamed("perSecond(a)", aPerSecond),
			getSeriesNamed("perSecond(b.*)", bPerSecond),
		},
		0,
		t,
	)
}
func TestPerSecondMultiMulti(t *testing.T) {
	testPerSecond(
		"multiple-serieslists",
		[][]models.Series{
			{
				getSeriesNamed("a", a),
				getSeriesNamed("b.foo{bar,baz}", b),
			},
			{
				getSeriesNamed("movingAverage(bar, '1min')", c),
			},
		},
		[]models.Series{
			getSeriesNamed("perSecond(a)", aPerSecond),
			getSeriesNamed("perSecond(b.foo{bar,baz})", bPerSecond),
			getSeriesNamed("perSecond(movingAverage(bar, '1min'))", cPerSecond),
		},
		0,
		t,
	)
}

func testPerSecond(name string, in [][]models.Series, out []models.Series, max int64, t *testing.T) {
	f := NewPerSecond()
	ps := f.(*FuncPerSecond)
	for i := range in {
		ps.in = append(ps.in, NewMock(in[i]))
		ps.maxValue = max
	}

	inputCopy := make([][]models.Series, len(in)) // to later verify that it is unchanged
	for i := range in {
		inputCopy[i] = models.SeriesCopy(in[i])
	}

	dataMap := initDataMapMultiple(in)
	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		for i := range inputCopy {
			if err := equalOutput(inputCopy[i], in[i], nil, nil); err != nil {
				t.Fatalf("Case %s: Input was modified, err = %s", name, err)
			}
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", name, err)
		}
	})
}
