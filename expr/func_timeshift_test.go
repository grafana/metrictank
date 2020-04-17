package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func shiftInput(in []schema.Point, offset int) []schema.Point {
	out := getCopy(in)
	for i := range in {
		if offset < 0 {
			out[i].Ts -= uint32(offset * -1)
		} else {
			out[i].Ts += uint32(offset)
		}
	}
	return out
}

func TestTimeShiftSingle(t *testing.T) {
	offset := -600
	testTimeShift(
		"identity",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a",
				Target:     "a",
				Datapoints: shiftInput(a, -offset), // shift forward to avoid underflow
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "timeShift(a, \"10m\")",
				Datapoints: shiftInput(a, -offset*2),
			},
		},
		t,
		offset,
		"10m",
		true,
		false,
	)
}

func TestTimeShiftMultiple(t *testing.T) {
	offset := -600
	testTimeShift(
		"identity",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a",
				Target:     "a",
				Datapoints: shiftInput(a, -offset), // shift forward to avoid underflow
			},
			{
				Interval:   10,
				QueryPatt:  "b",
				Target:     "b",
				Datapoints: shiftInput(b, -offset), // shift forward to avoid underflow
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "timeShift(a, \"10m\")",
				Datapoints: shiftInput(a, -offset*2),
			},
			{
				Interval:   10,
				QueryPatt:  "timeShift(b, \"10m\")",
				Datapoints: shiftInput(b, -offset*2),
			},
		},
		t,
		offset,
		"10m",
		true,
		false,
	)
}

func TestTimeShiftPositive(t *testing.T) {
	offset := 600
	testTimeShift(
		"identity",
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a",
				Target:     "a",
				Datapoints: shiftInput(a, offset), // shift forward to avoid underflow
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "timeShift(a, \"+10m\")",
				Datapoints: shiftInput(a, 0),
			},
		},
		t,
		offset,
		"+10m",
		true,
		false,
	)
}

func testTimeShift(name string, in []models.Series, out []models.Series, t *testing.T, expectedOffset int, shift string, resetEnd, alignDST bool) {
	f := NewTimeShift()
	f.(*FuncTimeShift).in = NewMock(in)
	f.(*FuncTimeShift).timeShift = shift
	f.(*FuncTimeShift).resetEnd = resetEnd
	f.(*FuncTimeShift).alignDST = alignDST

	// Much of the important logic is in the context
	context := Context{
		from: in[0].Datapoints[0].Ts,
		to:   in[0].Datapoints[len(in[0].Datapoints)-1].Ts,
	}
	newContext := f.Context(context)

	actualOffset := int(newContext.from) - int(context.from)
	if actualOffset != expectedOffset {
		t.Fatalf("case %q: Expected context offset = %d, got %d", name, expectedOffset, actualOffset)
	}

	gots, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(gots) != len(out) {
		t.Fatalf("case %q: timeShift len output expected %d, got %d", name, len(out), len(gots))
	}
	for i, g := range gots {
		exp := out[i]
		if g.QueryPatt != exp.QueryPatt {
			t.Fatalf("case %q : expected target %q, got %q", name, exp.QueryPatt, g.QueryPatt)
		}
		if len(g.Datapoints) != len(exp.Datapoints) {
			t.Fatalf("case %q len output expected %d, got %d", name, len(exp.Datapoints), len(g.Datapoints))
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

func BenchmarkTimeShift10k_1NoNulls(b *testing.B) {
	benchmarkTimeShift(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkTimeShift10k_10NoNulls(b *testing.B) {
	benchmarkTimeShift(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkTimeShift10k_100NoNulls(b *testing.B) {
	benchmarkTimeShift(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkTimeShift10k_1000NoNulls(b *testing.B) {
	benchmarkTimeShift(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkTimeShift10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkTimeShift(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTimeShift10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkTimeShift(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTimeShift10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkTimeShift(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTimeShift10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkTimeShift(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkTimeShift10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkTimeShift(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTimeShift10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkTimeShift(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTimeShift10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkTimeShift(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkTimeShift10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkTimeShift(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkTimeShift(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
	dataMap := DataMap(make(map[Req][]models.Series))
	for i := 0; i < b.N; i++ {
		f := NewTimeShift()
		f.(*FuncTimeShift).in = NewMock(input)
		f.(*FuncTimeShift).timeShift = "-1h"
		got, err := f.Exec(dataMap)
		if err != nil {
			b.Fatalf("%s", err)
		}
		dataMap.Clean()
		results = got
	}
}
