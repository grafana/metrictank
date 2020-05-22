package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func getNewRound(in []models.Series, precision int) *FuncRound {
	f := NewRound()
	ps := f.(*FuncRound)
	ps.in = NewMock(in)
	ps.precision = int64(precision)
	return ps
}

var lowPrec = []schema.Point{
	{Val: 1.25, Ts: 10},
	{Val: 2.5, Ts: 20},
	{Val: -2.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 8.00, Ts: 50},
	{Val: 1234567895.5, Ts: 60},
}

var lowPrecR0 = []schema.Point{
	{Val: 1, Ts: 10},
	{Val: 3, Ts: 20},
	{Val: -3, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 8, Ts: 50},
	{Val: 1234567896, Ts: 60},
}
var lowPrecR1 = []schema.Point{
	{Val: 1.3, Ts: 10},
	{Val: 2.5, Ts: 20},
	{Val: -2.5, Ts: 30},
	{Val: math.NaN(), Ts: 40},
	{Val: 8, Ts: 50},
	{Val: 1234567895.5, Ts: 60},
}
var lowPrecR2 = getCopy(lowPrec)
var lowPrecR20 = getCopy(lowPrec)

var highPrec = []schema.Point{
	{Val: 1.12345, Ts: 10},
	{Val: 3.987654, Ts: 20},
	{Val: -5.595959, Ts: 30},
	{Val: math.Inf(-1), Ts: 40},
	{Val: 8, Ts: 50},
	{Val: 1234567895634334.345, Ts: 60},
}
var highPrecRN1 = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 0, Ts: 20},
	{Val: -10, Ts: 30},
	{Val: math.Inf(-1), Ts: 40},
	{Val: 10, Ts: 50},
	{Val: 1234567895634330, Ts: 60},
}

var highPrecR0 = []schema.Point{
	{Val: 1, Ts: 10},
	{Val: 4, Ts: 20},
	{Val: -6, Ts: 30},
	{Val: math.Inf(-1), Ts: 40},
	{Val: 8, Ts: 50},
	{Val: 1234567895634334, Ts: 60},
}
var highPrecR1 = []schema.Point{
	{Val: 1.1, Ts: 10},
	{Val: 4, Ts: 20},
	{Val: -5.6, Ts: 30},
	{Val: math.Inf(-1), Ts: 40},
	{Val: 8, Ts: 50},
	{Val: 1234567895634334.3, Ts: 60},
}
var highPrecR2 = []schema.Point{
	{Val: 1.12, Ts: 10},
	{Val: 3.99, Ts: 20},
	{Val: -5.6, Ts: 30},
	{Val: math.Inf(-1), Ts: 40},
	{Val: 8, Ts: 50},
	{Val: 1234567895634334.34, Ts: 60},
}

var highPrecR20 = getCopy(highPrec)

type TestCase struct {
	precision      int
	expectedName   string
	expectedOutput []schema.Point
}

func TestRoundLowPrecInput(t *testing.T) {
	input := models.Series{
		Interval:   10,
		QueryPatt:  "lowPrec",
		Target:     "lowPrec",
		Datapoints: getCopy(lowPrec),
	}

	testData := []TestCase{
		{
			precision:      0,
			expectedName:   "round(lowPrec,0)",
			expectedOutput: lowPrecR0,
		}, {
			precision:      1,
			expectedName:   "round(lowPrec,1)",
			expectedOutput: lowPrecR1,
		}, {
			precision:      2,
			expectedName:   "round(lowPrec,2)",
			expectedOutput: lowPrecR2,
		}, {
			precision:      20,
			expectedName:   "round(lowPrec,20)",
			expectedOutput: lowPrecR20,
		},
	}

	checkCases(t, []models.Series{input}, testData)
}

func TestRoundHighPrecInput(t *testing.T) {
	input := models.Series{
		Interval:   10,
		QueryPatt:  "highPrec",
		Target:     "highPrec",
		Datapoints: getCopy(highPrec),
	}

	testData := []TestCase{
		{
			precision:      -1,
			expectedName:   "round(highPrec,-1)",
			expectedOutput: highPrecRN1,
		},
		{
			precision:      0,
			expectedName:   "round(highPrec,0)",
			expectedOutput: highPrecR0,
		}, {
			precision:      1,
			expectedName:   "round(highPrec,1)",
			expectedOutput: highPrecR1,
		}, {
			precision:      2,
			expectedName:   "round(highPrec,2)",
			expectedOutput: highPrecR2,
		}, {
			precision:      20,
			expectedName:   "round(highPrec,20)",
			expectedOutput: highPrecR20,
		},
	}

	checkCases(t, []models.Series{input}, testData)
}

func TestRoundOverflow(t *testing.T) {
	massiveDatapoints := []schema.Point{
		{Val: 1.0e+306, Ts: 10},
		{Val: -1.0e+306, Ts: 20},
	}
	input := models.Series{
		Interval:   10,
		QueryPatt:  "overflow",
		Target:     "overflow",
		Datapoints: getCopy(massiveDatapoints),
	}

	testData := []TestCase{
		{
			precision:      0,
			expectedName:   "round(overflow,0)",
			expectedOutput: massiveDatapoints,
		}, {
			precision:      1,
			expectedName:   "round(overflow,1)",
			expectedOutput: massiveDatapoints,
		}, {
			precision:      2,
			expectedName:   "round(overflow,2)",
			expectedOutput: massiveDatapoints,
		}, {
			precision:      17,
			expectedName:   "round(overflow,17)",
			expectedOutput: massiveDatapoints,
		},
	}

	for _, data := range testData {
		f := getNewRound([]models.Series{input}, data.precision)
		out := []models.Series{{
			Interval:   10,
			QueryPatt:  data.expectedName,
			Target:     data.expectedName,
			Datapoints: data.expectedOutput,
		}}
		got, err := f.Exec(make(map[Req][]models.Series))
		if err := equalOutput(out, got, nil, err); err != nil {
			t.Fatal("Failed test:", data.expectedName, err)
		}
	}
}

func TestRoundUnderflow(t *testing.T) {
	minisculeDatapoint := []schema.Point{
		{Val: 1.0e-306, Ts: 10},
		{Val: -1.0e-306, Ts: 20},
	}
	zeroes := []schema.Point{
		{Val: 0, Ts: 10},
		{Val: 0, Ts: 20},
	}
	input := models.Series{
		Interval:   10,
		QueryPatt:  "underflow",
		Target:     "underflow",
		Datapoints: getCopy(minisculeDatapoint),
	}

	testData := []TestCase{
		{
			precision:      0,
			expectedName:   "round(underflow,0)",
			expectedOutput: zeroes,
		}, {
			precision:      -17,
			expectedName:   "round(underflow,-17)",
			expectedOutput: zeroes,
		},
	}

	checkCases(t, []models.Series{input}, testData)
}

func TestRoundTiny(t *testing.T) {
	minisculeDatapoint := []schema.Point{
		{Val: 1.123e-36, Ts: 10},
		{Val: -1.123e-36, Ts: 20},
	}
	expected := []schema.Point{
		{Val: 1.12e-36, Ts: 10},
		{Val: -1.12e-36, Ts: 20},
	}
	input := models.Series{
		Interval:   10,
		QueryPatt:  "underflow",
		Target:     "underflow",
		Datapoints: getCopy(minisculeDatapoint),
	}

	testData := []TestCase{
		{
			precision:      38,
			expectedName:   "round(underflow,38)",
			expectedOutput: expected,
		},
	}

	checkCases(t, []models.Series{input}, testData)
}

func checkCases(t *testing.T, input []models.Series, cases []TestCase) {
	for _, c := range cases {
		f := getNewRound(input, c.precision)
		out := []models.Series{getSeriesNamed(c.expectedName, c.expectedOutput)}

		// Copy input to check that it is unchanged later
		inputCopy := make([]models.Series, len(input))
		copy(inputCopy, input)

		dataMap := initDataMap(input)

		got, err := f.Exec(dataMap)
		if err := equalOutput(out, got, nil, err); err != nil {
			t.Fatal(err)
		}

		t.Run("DidNotModifyInput", func(t *testing.T) {
			if err := equalOutput(inputCopy, input, nil, nil); err != nil {
				t.Fatalf("Input was modified, err = %s", err)
			}
		})

		t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
			if err := dataMap.CheckForOverlappingPoints(); err != nil {
				t.Fatalf("Point slices in datamap overlap, err = %s", err)
			}
		})
	}
}

func BenchmarkRound10k_1NoNulls(b *testing.B) {
	benchmarkRound(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRound10k_10NoNulls(b *testing.B) {
	benchmarkRound(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRound10k_100NoNulls(b *testing.B) {
	benchmarkRound(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRound10k_1000NoNulls(b *testing.B) {
	benchmarkRound(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRound10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRound(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRound10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRound(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRound10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRound(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRound10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRound(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRound10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkRound(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRound10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkRound(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRound10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkRound(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRound10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkRound(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkRound(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewRound()
		f.(*FuncRound).in = NewMock(input)
		f.(*FuncRound).precision = 2
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
