package expr

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

type ConstantLineTestCase struct {
	name 	string
	value 	float64
}

// array of time ranges
// 1s, 1m, 1hr, 1day, 30days, 400days
var timeRanges = []uint32{
	1,
	60,
	3600,
	86400,
	2592000,
	34560000,
}
func TestConstantLineSmallInt(t *testing.T) {
	cases := []ConstantLineTestCase {
		{
			name: "constantLine(1)",
			value: 1,
		},
		{
			name: "constantLine(100)",
			value: 100,
		},
		{
			name: "constantLine(10000)",
			value: 10000,
		},
	}

	for _, to := range timeRanges {
		testConstantLineWrapper(cases, 0, to, t)
	}
}

func TestConstantLineSmallFloatLowPrec(t *testing.T) {
	cases := []ConstantLineTestCase {
		{
			name: "constantLine(1.234)",
			value: 1.234,
		},
		{
			name: "constantLine(100.234)",
			value: 100.234,
		},
		{
			name: "constantLine(10000.234)",
			value: 10000.234,
		},
	}

	for _, to := range timeRanges {
		testConstantLineWrapper(cases, 0, to, t)
	}
}

func TestConstantLineSmallFloatHighPrec(t *testing.T) {
	cases := []ConstantLineTestCase {
		{
			name: "constantLine(1.2345678912345)",
			value: 1.2345678912345,
		},
		{
			name: "constantLine(100.2345678912345)",
			value: 100.2345678912345,
		},
		{
			name: "constantLine(10000.2345678912345)",
			value: 10000.2345678912345,
		},
	}

	for _, to := range timeRanges {
		testConstantLineWrapper(cases, 0, to, t)
	}
}


func TestConstantLineLargeInt(t *testing.T) {
	cases := []ConstantLineTestCase {
		{
			name: "constantLine(1000000000)",
			value: 1000000000,
		},
		{
			name: "constantLine(1000000000000)",
			value: 1000000000000,
		},
	}

	for _, to := range timeRanges {
		testConstantLineWrapper(cases, 0, to, t)
	}
}

func TestConstantLineSmallFloatLowPrec(t *testing.T) {
	cases := []ConstantLineTestCase {
		{
			name: "constantLine(1000000000.234)",
			value: 1000000000.234,
		},
		{
			name: "constantLine(1000000000000.234)",
			value: 1000000000000.234,
		},
	}

	for _, to := range timeRanges {
		testConstantLineWrapper(cases, 0, to, t)
	}
}

func TestConstantLineSmallFloatHighPrec(t *testing.T) {
	cases := []ConstantLineTestCase {
		{
			name: "constantLine(1000000000.2345678912345)",
			value: 1000000000.2345678912345,
		},
		{
			name: "constantLine(1000000000000.2345678912345)",
			value: 1000000000000.2345678912345,
		},
	}

	for _, to := range timeRanges {
		testConstantLineWrapper(cases, 0, to, t)
	}
}

func TestConstantLineFloatTooManyDecimals(t *testing.T) {
	cases := []ConstantLineTestCase {
		{
			name: "constantLine(1.23456789123456789123456789)",
			value: 1.23456789123456789123456789,
		},
	}

	for _, to := range timeRanges {
		testConstantLineWrapper(cases, 0, to, t)
	}
}

func testConstantLineWrapper(cases []ConstantLineTestCase, from uint32, to uint32, t *testing.T) {
	for _, c := range cases {
		testConstantLine(c.name, c.value, from, to, makeConstantLineSeries(c.value, from, to), t)
	}
}

func makeConstantLineSeries(value float64, from uint32, to uint32) ([]models.Series) {
	series := []models.Series{
		{
			Target:		 fmt.Sprintf("%g", value),
			QueryPatt:   fmt.Sprintf("%g", value),
			Datapoints: []schema.Point{
				{Val: value, Ts: from},
				{Val: value, Ts: from + uint32((to-from)/2.0)},
				{Val: value, Ts: to},
			},
		},
	}

	return series
}

func testConstantLine(name string, value float64, from uint32, to uint32, out []models.Series, t *testing.T) {
	f := NewConstantLine()
	f.(*FuncConstantLine).value  = value
	f.(*FuncConstantLine).from = from
	f.(*FuncConstantLine).to = to
	got, err := f.Exec(make(map[Req][]models.Series))

	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Failed test %q: , got %q", name, err)
	}
}

func BenchmarkConstantLine10k_1NoNulls(b *testing.B) {
	benchmarkConstantLine(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkConstantLine10k_10NoNulls(b *testing.B) {
	benchmarkConstantLine(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkConstantLine10k_100NoNulls(b *testing.B) {
	benchmarkConstantLine(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkConstantLine10k_1000NoNulls(b *testing.B) {
	benchmarkConstantLine(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkConstantLine10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkConstantLine10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkConstantLine(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkConstantLine(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewConstantLine()
		f.(*FuncConstantLine).value = 1.0
		f.(*FuncConstantLine).from = 1584849600
		f.(*FuncConstantLine).to = 1584849660
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
