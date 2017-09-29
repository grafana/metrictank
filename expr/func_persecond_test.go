package expr

import (
	"math"
	"testing"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
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
				{
					Interval:   10,
					QueryPatt:  "a",
					Target:     "a",
					Datapoints: getCopy(a),
				},
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "perSecond(a)",
				Target:     "perSecond(a)",
				Datapoints: getCopy(aPerSecond),
			},
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
				{
					Interval:   10,
					QueryPatt:  "counter8bit",
					Target:     "counter8bit",
					Datapoints: getCopy(d),
				},
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "perSecond(counter8bit)",
				Target:     "perSecond(counter8bit)",
				Datapoints: getCopy(dPerSecondMax255),
			},
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
				{
					Interval:   10,
					QueryPatt:  "a",
					Target:     "a",
					Datapoints: getCopy(a),
				},
				{
					Interval:   10,
					QueryPatt:  "b.*",
					Target:     "b.foo",
					Datapoints: getCopy(b),
				},
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "perSecond(a)",
				Target:     "perSecond(a)",
				Datapoints: getCopy(aPerSecond),
			},
			{
				Interval:   10,
				QueryPatt:  "perSecond(b.*)",
				Target:     "perSecond(b.foo)",
				Datapoints: getCopy(bPerSecond),
			},
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
				{
					Interval:   10,
					QueryPatt:  "a",
					Target:     "a",
					Datapoints: getCopy(a),
				},
				{
					Interval:   10,
					QueryPatt:  "b.foo{bar,baz}",
					Target:     "b.foobaz",
					Datapoints: getCopy(b),
				},
			},
			{
				{
					Interval:   10,
					QueryPatt:  "movingAverage(bar, '1min')",
					Target:     "movingAverage(bar, '1min')",
					Datapoints: getCopy(c),
				},
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "perSecond(a)",
				Target:     "perSecond(a)",
				Datapoints: getCopy(aPerSecond),
			},
			{
				Interval:   10,
				QueryPatt:  "perSecond(b.foo{bar,baz})",
				Target:     "perSecond(b.foobaz)",
				Datapoints: getCopy(bPerSecond),
			},
			{
				Interval:   10,
				QueryPatt:  "perSecond(movingAverage(bar, '1min'))",
				Target:     "perSecond(movingAverage(bar, '1min'))",
				Datapoints: getCopy(cPerSecond),
			},
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
	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("case %q: %s", name, err)
	}
}
