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
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
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
		[]models.Series{
			{
				Interval:   10,
				Target:     "counter8bit",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:   10,
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
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b.*",
				Datapoints: getCopy(b),
			},
		},
		[]models.Series{
			{
				Target:     "perSecond(a)",
				Datapoints: getCopy(aPerSecond),
			},
			{
				Target:     "perSecond(b.*)",
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
		[]models.Series{
			{
				Interval:   10,
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "b.foo{bar,baz}",
				Datapoints: getCopy(b),
			},

			{
				Interval:   10,
				Target:     "movingAverage(bar, '1min')",
				Datapoints: getCopy(c),
			},
		},
		[]models.Series{
			{
				Target:     "perSecond(a)",
				Datapoints: getCopy(aPerSecond),
			},
			{
				Target:     "perSecond(b.foo{bar,baz})",
				Datapoints: getCopy(bPerSecond),
			},
			{
				Target:     "perSecond(movingAverage(bar, '1min'))",
				Datapoints: getCopy(cPerSecond),
			},
		},
		0,
		t,
	)
}

func testPerSecond(name string, input []models.Series, out []models.Series, max float64, t *testing.T) {
	f := FuncPerSecond{}
	gots, err := f.Exec(make(map[Req][]models.Series), max, input)
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(gots) != len(out) {
		t.Fatalf("case %q: perSecond len output expected %d, got %d", name, len(out), len(gots))
	}
	for i, g := range gots {
		exp := out[i]
		if g.Target != exp.Target {
			t.Fatalf("case %q: expected target %q, got %q", name, exp.Target, g.Target)
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
