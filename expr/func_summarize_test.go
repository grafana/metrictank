package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	schema "gopkg.in/raintank/schema.v1"
)

var abSummarize = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 15},
	{Val: 0, Ts: 20},
	{Val: math.MaxFloat64, Ts: 25},
	{Val: 5.5, Ts: 30},
	{Val: math.MaxFloat64 - 20, Ts: 35},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 55},
	{Val: math.NaN(), Ts: 50},
	{Val: 1234567890, Ts: 55},
	{Val: 1234567890, Ts: 60},
	{Val: math.NaN(), Ts: 65},
}

var abcSummarize = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 12},
	{Val: 0, Ts: 14},
	{Val: math.NaN(), Ts: 16},
	{Val: math.NaN(), Ts: 18},
	{Val: 0, Ts: 20},
	{Val: math.MaxFloat64, Ts: 22},
	{Val: 0, Ts: 24},
	{Val: math.NaN(), Ts: 26},
	{Val: math.NaN(), Ts: 28},
	{Val: 5.5, Ts: 30},
	{Val: math.MaxFloat64 - 20, Ts: 32},
	{Val: 1, Ts: 34},
	{Val: math.NaN(), Ts: 36},
	{Val: math.NaN(), Ts: 38},
	{Val: math.NaN(), Ts: 40},
	{Val: math.NaN(), Ts: 42},
	{Val: 2, Ts: 44},
	{Val: math.NaN(), Ts: 46},
	{Val: math.NaN(), Ts: 48},
	{Val: math.NaN(), Ts: 50},
	{Val: 1234567890, Ts: 52},
	{Val: 3, Ts: 54},
	{Val: math.NaN(), Ts: 56},
	{Val: math.NaN(), Ts: 58},
	{Val: 1234567890, Ts: 60},
	{Val: math.NaN(), Ts: 62},
	{Val: 4, Ts: 64},
}

func TestSummarizeDefaultInterval(t *testing.T) {
	input := []models.Series{
		{
			Target:     "a",
			QueryPatt:  "a",
			QueryFrom:  10,
			QueryTo:    60,
			Interval:   10,
			Datapoints: getCopy(a),
		},
	}
	outputSum := [][]models.Series{
		{
			{
				Target:     "summarize(a, \"10\", \"sum\")",
				QueryPatt:  "summarize(a, \"10\", \"sum\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
		},
		{
			{
				Target:     "summarize(a, \"10\", \"sum\", true)",
				QueryPatt:  "summarize(a, \"10\", \"sum\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
		},
	}
	outputMax := [][]models.Series{
		{
			{
				Target:     "summarize(a, \"10\", \"max\")",
				QueryPatt:  "summarize(a, \"10\", \"max\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
		},
		{
			{
				Target:     "summarize(a, \"10\", \"max\", true)",
				QueryPatt:  "summarize(a, \"10\", \"max\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
		},
	}
	// Note that graphite does not accept 10 as default seconds, but dur lib defaults to seconds without units!
	testSummarize("Default Interval", input, outputSum[0], "10", "sum", false, t)
	testSummarize("Default Interval", input, outputSum[1], "10", "sum", true, t)
	testSummarize("Default Interval", input, outputMax[0], "10", "max", false, t)
	testSummarize("Default Interval", input, outputMax[1], "10", "max", true, t)
}

func TestSummarizeOversampled(t *testing.T) {

	var aOversampled = []schema.Point{
		{Val: 0, Ts: 10},
		{Val: math.NaN(), Ts: 15},
		{Val: 0, Ts: 20},
		{Val: math.NaN(), Ts: 25},
		{Val: 5.5, Ts: 30},
		{Val: math.NaN(), Ts: 35},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 45},
		{Val: math.NaN(), Ts: 50},
		{Val: math.NaN(), Ts: 55},
		{Val: 1234567890, Ts: 60},
	}

	input := []models.Series{
		{
			Target:     "a",
			QueryPatt:  "a",
			QueryFrom:  10,
			QueryTo:    60,
			Interval:   10,
			Datapoints: getCopy(a),
		},
	}
	outputSum := [][]models.Series{
		{
			{
				Target:     "summarize(a, \"5\", \"sum\")",
				QueryPatt:  "summarize(a, \"5\", \"sum\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   5,
				Datapoints: getCopy(aOversampled),
			},
		},
		{
			{
				Target:     "summarize(a, \"5\", \"sum\", true)",
				QueryPatt:  "summarize(a, \"5\", \"sum\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   5,
				Datapoints: getCopy(aOversampled),
			},
		},
	}
	outputMax := [][]models.Series{
		{
			{
				Target:     "summarize(a, \"5\", \"max\")",
				QueryPatt:  "summarize(a, \"5\", \"max\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   5,
				Datapoints: getCopy(aOversampled),
			},
		},
		{
			{
				Target:     "summarize(a, \"5\", \"max\", true)",
				QueryPatt:  "summarize(a, \"5\", \"max\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   5,
				Datapoints: getCopy(aOversampled),
			},
		},
	}
	// Note that graphite does not accept 10 as default seconds, but dur lib defaults to seconds without units!
	testSummarize("Oversampled Identity", input, outputSum[0], "5", "sum", false, t)
	testSummarize("Oversampled Identity", input, outputSum[1], "5", "sum", true, t)
	testSummarize("Oversampled Identity", input, outputMax[0], "5", "max", false, t)
	testSummarize("Oversampled Identity", input, outputMax[1], "5", "max", true, t)
}

func TestSummarizeNyquistSingleIdentity(t *testing.T) {
	input := []models.Series{
		{
			Target:     "a",
			QueryPatt:  "a",
			QueryFrom:  10,
			QueryTo:    60,
			Interval:   10,
			Datapoints: getCopy(a),
		},
	}
	outputSum := [][]models.Series{
		{
			{
				Target:     "summarize(a, \"10s\", \"sum\")",
				QueryPatt:  "summarize(a, \"10s\", \"sum\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
		},
		{
			{
				Target:     "summarize(a, \"10s\", \"sum\", true)",
				QueryPatt:  "summarize(a, \"10s\", \"sum\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
		},
	}
	outputMax := [][]models.Series{
		{
			{
				Target:     "summarize(a, \"10s\", \"max\")",
				QueryPatt:  "summarize(a, \"10s\", \"max\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
		},
		{
			{
				Target:     "summarize(a, \"10s\", \"max\", true)",
				QueryPatt:  "summarize(a, \"10s\", \"max\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
		},
	}
	testSummarize("Nyquist Single Identity", input, outputSum[0], "10s", "sum", false, t)
	testSummarize("Nyquist Single Identity", input, outputSum[1], "10s", "sum", true, t)
	testSummarize("Nyquist Single Identity", input, outputMax[0], "10s", "max", false, t)
	testSummarize("Nyquist Single Identity", input, outputMax[1], "10s", "max", true, t)
}

func TestSummarizeMultipleIdentity(t *testing.T) {
	input := []models.Series{
		{
			Target:     "a",
			QueryPatt:  "a",
			QueryFrom:  10,
			QueryTo:    60,
			Interval:   10,
			Datapoints: getCopy(a),
		},
		{
			Target:     "b",
			QueryPatt:  "b",
			QueryFrom:  10,
			QueryTo:    60,
			Interval:   10,
			Datapoints: getCopy(b),
		},
	}
	outputSum := [][]models.Series{
		{
			{
				Target:     "summarize(a, \"10s\", \"sum\")",
				QueryPatt:  "summarize(a, \"10s\", \"sum\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
			{
				Target:     "summarize(b, \"10s\", \"sum\")",
				QueryPatt:  "summarize(b, \"10s\", \"sum\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(b),
			},
		},
		{
			{
				Target:     "summarize(a, \"10s\", \"sum\", true)",
				QueryPatt:  "summarize(a, \"10s\", \"sum\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
			{
				Target:     "summarize(b, \"10s\", \"sum\", true)",
				QueryPatt:  "summarize(b, \"10s\", \"sum\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(b),
			},
		},
	}
	outputMax := [][]models.Series{
		{
			{
				Target:     "summarize(a, \"10s\", \"max\")",
				QueryPatt:  "summarize(a, \"10s\", \"max\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
			{
				Target:     "summarize(b, \"10s\", \"max\")",
				QueryPatt:  "summarize(b, \"10s\", \"max\")",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(b),
			},
		},
		{
			{
				Target:     "summarize(a, \"10s\", \"max\", true)",
				QueryPatt:  "summarize(a, \"10s\", \"max\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(a),
			},
			{
				Target:     "summarize(b, \"10s\", \"max\", true)",
				QueryPatt:  "summarize(b, \"10s\", \"max\", true)",
				QueryFrom:  10,
				QueryTo:    60,
				Interval:   10,
				Datapoints: getCopy(b),
			},
		},
	}

	testSummarize("identity Multiple", input, outputSum[0], "10s", "sum", false, t)
	testSummarize("identity Multiple", input, outputSum[1], "10s", "sum", true, t)
	testSummarize("identity Multiple", input, outputMax[0], "10s", "max", false, t)
	testSummarize("identity Multiple", input, outputMax[1], "10s", "max", true, t)
}

func TestSummarizeConsolidated(t *testing.T) {
	input := []models.Series{
		{
			Target:     "ab",
			QueryPatt:  "ab",
			QueryFrom:  10,
			QueryTo:    65,
			Interval:   5,
			Datapoints: getCopy(abSummarize),
		},
		{
			Target:     "abc",
			QueryPatt:  "abc",
			QueryFrom:  10,
			QueryTo:    64,
			Interval:   2,
			Datapoints: getCopy(abcSummarize),
		},
	}
	outputSum := [][]models.Series{
		{
			{
				Target:     "summarize(ab, \"10s\", \"sum\")",
				QueryPatt:  "summarize(ab, \"10s\", \"sum\")",
				QueryFrom:  10,
				QueryTo:    65,
				Interval:   10,
				Datapoints: getCopy(sumab),
			},
			{
				Target:     "summarize(abc, \"10s\", \"sum\")",
				QueryPatt:  "summarize(abc, \"10s\", \"sum\")",
				QueryFrom:  10,
				QueryTo:    64,
				Interval:   10,
				Datapoints: getCopy(sumabc),
			},
		},
		{
			{
				Target:     "summarize(ab, \"10s\", \"sum\", true)",
				QueryPatt:  "summarize(ab, \"10s\", \"sum\", true)",
				QueryFrom:  10,
				QueryTo:    65,
				Interval:   10,
				Datapoints: getCopy(sumab),
			},
			{
				Target:     "summarize(abc, \"10s\", \"sum\", true)",
				QueryPatt:  "summarize(abc, \"10s\", \"sum\", true)",
				QueryFrom:  10,
				QueryTo:    64,
				Interval:   10,
				Datapoints: getCopy(sumabc),
			},
		},
	}
	outputMax := [][]models.Series{
		{
			{
				Target:     "summarize(ab, \"10s\", \"max\")",
				QueryPatt:  "summarize(ab, \"10s\", \"max\")",
				QueryFrom:  10,
				QueryTo:    65,
				Interval:   10,
				Datapoints: getCopy(maxab),
			},
			{
				Target:     "summarize(abc, \"10s\", \"max\")",
				QueryPatt:  "summarize(abc, \"10s\", \"max\")",
				QueryFrom:  10,
				QueryTo:    64,
				Interval:   10,
				Datapoints: getCopy(maxabc),
			},
		},
		{
			{
				Target:     "summarize(ab, \"10s\", \"max\", true)",
				QueryPatt:  "summarize(ab, \"10s\", \"max\", true)",
				QueryFrom:  10,
				QueryTo:    65,
				Interval:   10,
				Datapoints: getCopy(maxab),
			},
			{
				Target:     "summarize(abc, \"10s\", \"max\", true)",
				QueryPatt:  "summarize(abc, \"10s\", \"max\", true)",
				QueryFrom:  10,
				QueryTo:    64,
				Interval:   10,
				Datapoints: getCopy(maxabc),
			},
		},
	}
	testSummarize("Consolidated", input, outputSum[0], "10s", "sum", false, t)
	testSummarize("Consolidated", input, outputSum[1], "10s", "sum", true, t)
	testSummarize("Consolidated", input, outputMax[0], "10s", "max", false, t)
	testSummarize("Consolidated", input, outputMax[1], "10s", "max", true, t)
}

// Tests misaligned QueryFrom/QueryTo with Interval and IntervalString
func TestSummarizeMisAligned(t *testing.T) {
	input := []models.Series{
		{
			Target:     "ab",
			QueryPatt:  "ab",
			QueryFrom:  7,
			QueryTo:    67,
			Interval:   5,
			Datapoints: getCopy(abSummarize),
		},
		{
			Target:     "abc",
			QueryPatt:  "abc",
			QueryFrom:  1,
			QueryTo:    67,
			Interval:   2,
			Datapoints: getCopy(abcSummarize),
		},
	}
	outputSum := [][]models.Series{
		{
			{
				Target:     "summarize(ab, \"10s\", \"sum\")",
				QueryPatt:  "summarize(ab, \"10s\", \"sum\")",
				QueryFrom:  7,
				QueryTo:    67,
				Interval:   10,
				Datapoints: getCopy(sumab),
			},
			{
				Target:     "summarize(abc, \"10s\", \"sum\")",
				QueryPatt:  "summarize(abc, \"10s\", \"sum\")",
				QueryFrom:  1,
				QueryTo:    67,
				Interval:   10,
				Datapoints: getCopy(sumabc),
			},
		},
		{
			{
				Target:     "summarize(ab, \"10s\", \"sum\", true)",
				QueryPatt:  "summarize(ab, \"10s\", \"sum\", true)",
				QueryFrom:  7,
				QueryTo:    67,
				Interval:   10,
				Datapoints: getCopy(sumab),
			},
			{
				Target:     "summarize(abc, \"10s\", \"sum\", true)",
				QueryPatt:  "summarize(abc, \"10s\", \"sum\", true)",
				QueryFrom:  1,
				QueryTo:    67,
				Interval:   10,
				Datapoints: getCopy(sumabc),
			},
		},
	}
	outputMax := [][]models.Series{
		{
			{
				Target:     "summarize(ab, \"10s\", \"max\")",
				QueryPatt:  "summarize(ab, \"10s\", \"max\")",
				QueryFrom:  7,
				QueryTo:    67,
				Interval:   10,
				Datapoints: getCopy(maxab),
			},
			{
				Target:     "summarize(abc, \"10s\", \"max\")",
				QueryPatt:  "summarize(abc, \"10s\", \"max\")",
				QueryFrom:  1,
				QueryTo:    67,
				Interval:   10,
				Datapoints: getCopy(maxabc),
			},
		},
		{
			{
				Target:     "summarize(ab, \"10s\", \"max\", true)",
				QueryPatt:  "summarize(ab, \"10s\", \"max\", true)",
				QueryFrom:  7,
				QueryTo:    67,
				Interval:   10,
				Datapoints: getCopy(maxab),
			},
			{
				Target:     "summarize(abc, \"10s\", \"max\", true)",
				QueryPatt:  "summarize(abc, \"10s\", \"max\", true)",
				QueryFrom:  1,
				QueryTo:    67,
				Interval:   10,
				Datapoints: getCopy(maxabc),
			},
		},
	}

	testSummarize("MisAligned Multiple", input, outputSum[0], "10s", "sum", false, t)
	testSummarize("MisAligned Multiple", input, outputSum[1], "10s", "sum", true, t)
	testSummarize("MisAligned Multiple", input, outputMax[0], "10s", "max", false, t)
	testSummarize("MisAligned Multiple", input, outputMax[1], "10s", "max", true, t)
}

func TestSummarizeAlignToFrom(t *testing.T) {
	var aligned30 = []schema.Point{
		{Val: 1, Ts: 30},
		{Val: 2, Ts: 60},
		{Val: 3, Ts: 90},
		{Val: 4, Ts: 120},
		{Val: 5, Ts: 150},
		{Val: 6, Ts: 180},
		{Val: 7, Ts: 210},
		{Val: 8, Ts: 240},
	}
	var unaligned45sum, unaligned45max = []schema.Point{
		{Val: 1, Ts: 0},
		{Val: 2, Ts: 45},
		{Val: 7, Ts: 90},
		{Val: 5, Ts: 135},
		{Val: 13, Ts: 180},
		{Val: 8, Ts: 225},
	},
		[]schema.Point{
			{Val: 1, Ts: 0},
			{Val: 2, Ts: 45},
			{Val: 4, Ts: 90},
			{Val: 5, Ts: 135},
			{Val: 7, Ts: 180},
			{Val: 8, Ts: 225},
		}
	var aligned45sum, aligned45max = []schema.Point{
		{Val: 3, Ts: 30},
		{Val: 3, Ts: 75},
		{Val: 9, Ts: 120},
		{Val: 6, Ts: 165},
		{Val: 15, Ts: 210},
	},
		[]schema.Point{
			{Val: 2, Ts: 30},
			{Val: 3, Ts: 75},
			{Val: 5, Ts: 120},
			{Val: 6, Ts: 165},
			{Val: 8, Ts: 210},
		}

	input := []models.Series{
		{
			Target:     "align",
			QueryPatt:  "align",
			QueryFrom:  30,
			QueryTo:    240,
			Interval:   30,
			Datapoints: getCopy(aligned30),
		},
	}
	outputSum := [][]models.Series{
		{
			{
				Target:     "summarize(align, \"45s\", \"sum\")",
				QueryPatt:  "summarize(align, \"45s\", \"sum\")",
				QueryFrom:  30,
				QueryTo:    240,
				Interval:   45,
				Datapoints: getCopy(unaligned45sum),
			},
		},
		{
			{
				Target:     "summarize(align, \"45s\", \"sum\", true)",
				QueryPatt:  "summarize(align, \"45s\", \"sum\", true)",
				QueryFrom:  30,
				QueryTo:    240,
				Interval:   45,
				Datapoints: getCopy(aligned45sum),
			},
		},
	}
	outputMax := [][]models.Series{
		{
			{
				Target:     "summarize(align, \"45s\", \"max\")",
				QueryPatt:  "summarize(align, \"45s\", \"max\")",
				QueryFrom:  30,
				QueryTo:    240,
				Interval:   45,
				Datapoints: getCopy(unaligned45max),
			},
		},
		{
			{
				Target:     "summarize(align, \"45s\", \"max\", true)",
				QueryPatt:  "summarize(align, \"45s\", \"max\", true)",
				QueryFrom:  30,
				QueryTo:    240,
				Interval:   45,
				Datapoints: getCopy(aligned45max),
			},
		},
	}

	testSummarize("AlignToFrom", input, outputSum[0], "45s", "sum", false, t)
	testSummarize("AlignToFrom", input, outputSum[1], "45s", "sum", true, t)
	testSummarize("AlignToFrom", input, outputMax[0], "45s", "max", false, t)
	testSummarize("AlignToFrom", input, outputMax[1], "45s", "max", true, t)
}

func TestSummarizeLargeIntervalTimestamps(t *testing.T) {

	// This test is specifically testing the timestamp behavior, so we don't need real values.
	// However, we do need a lot of datapoints to trigger the bug we are regression testing against.
	inputInterval := uint32(10)
	numTimestamps := uint32(89*24*60*60) / inputInterval
	startTime := uint32(1464637518)
	endTime := startTime + numTimestamps*inputInterval

	inputDps := make([]schema.Point, 0, numTimestamps)

	for i := uint32(0); i < numTimestamps; i++ {
		inputDps = append(inputDps, schema.Point{Val: 0, Ts: startTime + i*inputInterval})
	}

	outputInterval := uint32(30 * 24 * 60 * 60)

	// alignToFrom = false - starting timestamp is a multiple of `outputInterval`
	unalignedStart := startTime - (startTime % outputInterval)
	var unalignedExpected = []schema.Point{
		{Val: 0, Ts: unalignedStart},
		{Val: 0, Ts: unalignedStart + outputInterval},
		{Val: 0, Ts: unalignedStart + 2*outputInterval},
		{Val: 0, Ts: unalignedStart + 3*outputInterval},
	}

	// alignToFrom = true - starting timestamp is unchanged from input
	var alignedExpected = []schema.Point{
		{Val: 0, Ts: startTime},
		{Val: 0, Ts: startTime + outputInterval},
		{Val: 0, Ts: startTime + 2*outputInterval},
	}

	input := []models.Series{
		{
			Target:     "align",
			QueryPatt:  "align",
			QueryFrom:  startTime,
			QueryTo:    endTime,
			Interval:   inputInterval,
			Datapoints: getCopy(inputDps),
		},
	}
	outputSum := [][]models.Series{
		{
			{
				Target:     "summarize(align, \"30d\", \"sum\")",
				QueryPatt:  "summarize(align, \"30d\", \"sum\")",
				QueryFrom:  startTime,
				QueryTo:    endTime,
				Interval:   outputInterval,
				Datapoints: getCopy(unalignedExpected),
			},
		},
		{
			{
				Target:     "summarize(align, \"30d\", \"sum\", true)",
				QueryPatt:  "summarize(align, \"30d\", \"sum\", true)",
				QueryFrom:  startTime,
				QueryTo:    endTime,
				Interval:   outputInterval,
				Datapoints: getCopy(alignedExpected),
			},
		},
	}

	testSummarize("LongIntervals", input, outputSum[0], "30d", "sum", false, t)
	testSummarize("LongIntervals", input, outputSum[1], "30d", "sum", true, t)
}

func testSummarize(name string, in []models.Series, out []models.Series, intervalString, fn string, alignToFrom bool, t *testing.T) {
	f := NewSummarize()

	summarize := f.(*FuncSummarize)
	summarize.in = NewMock(in)
	summarize.intervalString = intervalString
	summarize.fn = fn
	summarize.alignToFrom = alignToFrom

	gots, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}

	if len(gots) != len(out) {
		t.Fatalf("case %q (%q, %q, %t): len output expected %d, got %d", name, intervalString, fn, alignToFrom, len(out), len(gots))
	}

	for i, got := range gots {
		exp := out[i]
		if got.Target != exp.Target {
			t.Fatalf("case %q (%q, %q, %t): expected target %q, got %q", name, intervalString, fn, alignToFrom, exp.Target, got.Target)
		}
		if len(got.Datapoints) != len(exp.Datapoints) {
			t.Fatalf("case %q (%q, %q, %t): len output expected %v, got %v", name, intervalString, fn, alignToFrom, (exp.Datapoints), (got.Datapoints))
		}
		for j, p := range exp.Datapoints {
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(got.Datapoints[j].Val)
			if (bothNaN || p.Val == got.Datapoints[j].Val) && p.Ts == got.Datapoints[j].Ts {
				continue
			}

			t.Fatalf("case %q (%q, %q, %t): output point %d - expected %v got %v", name, intervalString, fn, alignToFrom, j, p, got.Datapoints[j])
		}
	}
}

func BenchmarkSummarize10k_1NoNulls(b *testing.B) {
	benchmarkSummarize(b, 1, test.RandFloats10k, test.RandFloats10k, "1h")
}
func BenchmarkSummarize10k_10NoNulls(b *testing.B) {
	benchmarkSummarize(b, 10, test.RandFloats10k, test.RandFloats10k, "1h")
}
func BenchmarkSummarize10k_100NoNulls(b *testing.B) {
	benchmarkSummarize(b, 100, test.RandFloats10k, test.RandFloats10k, "1h")
}
func BenchmarkSummarize10k_1000NoNulls(b *testing.B) {
	benchmarkSummarize(b, 1000, test.RandFloats10k, test.RandFloats10k, "1h")
}

func BenchmarkSummarize10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSummarize(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k, "1h")
}
func BenchmarkSummarize10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSummarize(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k, "1h")
}
func BenchmarkSummarize10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSummarize(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k, "1h")
}
func BenchmarkSummarize10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSummarize(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k, "1h")
}

func BenchmarkSummarize10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkSummarize(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k, "1h")
}
func BenchmarkSummarize10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkSummarize(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k, "1h")
}
func BenchmarkSummarize10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkSummarize(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k, "1h")
}
func BenchmarkSummarize10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkSummarize(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k, "1h")
}

func benchmarkSummarize(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point, intervalString string) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: strconv.Itoa(i),
		}
		if i%1 == 0 {
			series.Datapoints = fn0()
		} else {
			series.Datapoints = fn1()
		}
		input = append(input, series)
	}
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		f := NewSummarize()

		summarize := f.(*FuncSummarize)
		summarize.in = NewMock(input)
		summarize.intervalString = intervalString

		results, err = f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
	}
	b.SetBytes(int64(numSeries * len(results[0].Datapoints) * 12))
}
