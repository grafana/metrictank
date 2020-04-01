package expr

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

type ConstantLineTestCase struct {
	name  string
	value float64
}

func TestConstantLineSmallInt(t *testing.T) {
	cases := []ConstantLineTestCase{
		{
			name:  "constantLine(1)",
			value: 1,
		},
		{
			name:  "constantLine(100)",
			value: 100,
		},
		{
			name:  "constantLine(10000)",
			value: 10000,
		},
	}

	testConstantLineWrapper(cases, t)
}

func TestConstantLineSmallFloatLowPrec(t *testing.T) {
	cases := []ConstantLineTestCase{
		{
			name:  "constantLine(1.234)",
			value: 1.234,
		},
		{
			name:  "constantLine(100.234)",
			value: 100.234,
		},
		{
			name:  "constantLine(10000.234)",
			value: 10000.234,
		},
	}

	testConstantLineWrapper(cases, t)
}

func TestConstantLineSmallFloatHighPrec(t *testing.T) {
	cases := []ConstantLineTestCase{
		{
			name:  "constantLine(1.2345678912345)",
			value: 1.2345678912345,
		},
		{
			name:  "constantLine(100.2345678912345)",
			value: 100.2345678912345,
		},
		{
			name:  "constantLine(10000.2345678912345)",
			value: 10000.2345678912345,
		},
	}

	testConstantLineWrapper(cases, t)
}

func TestConstantLineLargeInt(t *testing.T) {
	cases := []ConstantLineTestCase{
		{
			name:  "constantLine(1000000000)",
			value: 1000000000,
		},
		{
			name:  "constantLine(1000000000000)",
			value: 1000000000000,
		},
	}

	testConstantLineWrapper(cases, t)
}

func TestConstantLineLargeFloatLowPrec(t *testing.T) {
	cases := []ConstantLineTestCase{
		{
			name:  "constantLine(1000000000.234)",
			value: 1000000000.234,
		},
		{
			name:  "constantLine(1000000000000.234)",
			value: 1000000000000.234,
		},
	}

	testConstantLineWrapper(cases, t)
}

func TestConstantLineLargeFloatHighPrec(t *testing.T) {
	cases := []ConstantLineTestCase{
		{
			name:  "constantLine(1000000000.2345678912345)",
			value: 1000000000.2345678912345,
		},
		{
			name:  "constantLine(1000000000000.2345678912345)",
			value: 1000000000000.2345678912345,
		},
	}

	testConstantLineWrapper(cases, t)
}

func TestConstantLineFloatTooManyDecimals(t *testing.T) {
	cases := []ConstantLineTestCase{
		{
			name:  "constantLine(1.23456789123456789123456789)",
			value: 1.23456789123456789123456789,
		},
	}

	testConstantLineWrapper(cases, t)
}

func testConstantLineWrapper(cases []ConstantLineTestCase, t *testing.T) {
	// array of time ranges
	// 1s, 1m, 1hr, 1day, 30days, 400days
	day := time.Hour * 24
	timeRanges := []time.Duration{
		time.Second,
		time.Second * 2,
		time.Minute,
		time.Hour,
		day,
		day * 30,
		day * 400,
	}

	for _, c := range cases {
		for _, to := range timeRanges {
			toInt := uint32(to.Seconds())
			testConstantLine(c.name, c.value, 0, toInt, makeConstantLineSeries(c.value, 0, toInt), t)
		}
	}
}

func makeConstantLineSeries(value float64, first uint32, last uint32) []models.Series {
	datapoints := []schema.Point{
		{Val: value, Ts: first},
	}
	diff := last - first
	if diff > 2 {
		datapoints = append(datapoints,
			schema.Point{Val: value, Ts: first + uint32(diff/2.0)},
			schema.Point{Val: value, Ts: last},
		)
	} else if diff == 2 {
		datapoints = append(datapoints, schema.Point{Val: value, Ts: first + 1})
	}
	series := []models.Series{
		{
			Target:     fmt.Sprintf("%g", value),
			QueryPatt:  fmt.Sprintf("%g", value),
			Datapoints: datapoints,
		},
	}

	return series
}

func testConstantLine(name string, value float64, from uint32, to uint32, out []models.Series, t *testing.T) {
	f := NewConstantLine()
	f.(*FuncConstantLine).value = value
	f.(*FuncConstantLine).first = from
	f.(*FuncConstantLine).last = to
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
		f.(*FuncConstantLine).first = 1584849600
		f.(*FuncConstantLine).last = 1584849660
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
