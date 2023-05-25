package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/internal/test"
	"github.com/grafana/metrictank/pkg/api/models"
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

func TestTimeShiftZero(t *testing.T) {
	offset := -600
	testTimeShift(
		"no_series",
		[]models.Series{},
		[]models.Series{},
		t,
		offset,
		"10m",
		true,
		false,
	)
}

func TestTimeShiftSingle(t *testing.T) {
	offset := -600
	input := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "a",
			Target:     "a",
			Datapoints: shiftInput(a, -offset), // shift forward to avoid underflow
		},
	}
	inputCopy := models.SeriesCopy(input) // to later verify that it is unchanged

	testTimeShift(
		"identity",
		input,
		[]models.Series{
			{
				Interval:   10,
				Target:     "timeShift(a, \"-10m\")",
				QueryPatt:  "timeShift(a, \"-10m\")",
				Datapoints: shiftInput(a, -offset*2),
			},
		},
		t,
		offset,
		"10m",
		true,
		false,
	)

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, input, nil, nil); err != nil {
			t.Fatal("Input was modified: ", err)
		}
	})
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
				Target:     "timeShift(a, \"-10m\")",
				QueryPatt:  "timeShift(a, \"-10m\")",
				Datapoints: shiftInput(a, -offset*2),
			},
			{
				Interval:   10,
				Target:     "timeShift(b, \"-10m\")",
				QueryPatt:  "timeShift(b, \"-10m\")",
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
				Target:     "timeShift(a, \"+10m\")",
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
		from: 1000,
		to:   2000,
	}
	if len(in) > 0 {
		context.from = in[0].Datapoints[0].Ts
		context.to = in[0].Datapoints[len(in[0].Datapoints)-1].Ts
	}

	newContext := f.Context(context)

	actualOffset := int(newContext.from) - int(context.from)
	if actualOffset != expectedOffset {
		t.Fatalf("case %q: Expected context offset = %d, got %d", name, expectedOffset, actualOffset)
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal("Failed test:", name, err)
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

func benchmarkTimeShift(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
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
