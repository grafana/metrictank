package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/test"
)

func TestOffsetToZero(t *testing.T) {
	const (
		positiveOffset = 10
		negativeOffset = -10
	)
	for _, tc := range []struct {
		name   string
		input  []models.Series
		output []models.Series
	}{
		{
			name:   "no input",
			input:  []models.Series{},
			output: []models.Series{},
		},
		{
			name: "single positive offset",
			input: []models.Series{
				getSeriesNamed("a", []schema.Point{
					{Val: 0 + positiveOffset, Ts: 10},
					{Val: 0 + positiveOffset, Ts: 20},
					{Val: 5.5 + positiveOffset, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.Inf(1), Ts: 50},
					{Val: 1234567890 + positiveOffset, Ts: 60},
				}),
			},
			output: []models.Series{
				getSeriesNamed("offsetToZero(a)", []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 5.5, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.Inf(1), Ts: 50},
					{Val: 1234567890, Ts: 60},
				}),
			},
		},
		{
			name: "single negative offset",
			input: []models.Series{
				getSeriesNamed("a", []schema.Point{
					{Val: 0 + negativeOffset, Ts: 10},
					{Val: 0 + negativeOffset, Ts: 20},
					{Val: 5.5 + negativeOffset, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.Inf(1), Ts: 50},
					{Val: 1234567890 + negativeOffset, Ts: 60},
				}),
			},
			output: []models.Series{
				getSeriesNamed("offsetToZero(a)", []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 5.5, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.Inf(1), Ts: 50},
					{Val: 1234567890, Ts: 60},
				}),
			},
		},
		{
			name: "negative infinity",
			input: []models.Series{
				getSeriesNamed("a", []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 5.5, Ts: 30},
					{Val: math.Inf(-1), Ts: 40},
					{Val: math.Inf(1), Ts: 50},
					{Val: 1234567890, Ts: 60},
				}),
			},
			output: []models.Series{
				getSeriesNamed("offsetToZero(a)", []schema.Point{
					{Val: math.Inf(1), Ts: 10},
					{Val: math.Inf(1), Ts: 20},
					{Val: math.Inf(1), Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.Inf(1), Ts: 50},
					{Val: math.Inf(1), Ts: 60},
				}),
			},
		},
		{
			name: "multiple",
			input: []models.Series{
				getSeriesNamed("a", []schema.Point{
					{Val: 0 + positiveOffset, Ts: 10},
					{Val: 0 + positiveOffset, Ts: 20},
					{Val: 5.5 + positiveOffset, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: 1234567890 + positiveOffset, Ts: 60},
				}),
				getSeriesNamed("b", []schema.Point{
					{Val: 0 + negativeOffset, Ts: 10},
					{Val: 42 + negativeOffset, Ts: 20},
					{Val: 288 + negativeOffset, Ts: 30},
					{Val: 1024 + negativeOffset, Ts: 40},
					{Val: 2048 + negativeOffset, Ts: 50},
					{Val: 1234567890 + negativeOffset, Ts: 60},
				}),
			},
			output: []models.Series{
				getSeriesNamed("offsetToZero(a)", []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 5.5, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: 1234567890, Ts: 60},
				}),
				getSeriesNamed("offsetToZero(b)", []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 42, Ts: 20},
					{Val: 288, Ts: 30},
					{Val: 1024, Ts: 40},
					{Val: 2048, Ts: 50},
					{Val: 1234567890, Ts: 60},
				}),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := NewOffsetToZero()
			f.(*FuncOffsetToZero).in = NewMock(tc.input)

			inputCopy := models.SeriesCopy(tc.input) // to later verify that it is unchanged

			dataMap := initDataMap(tc.input)

			got, err := f.Exec(dataMap)
			if err := equalOutput(tc.output, got, nil, err); err != nil {
				t.Fatalf("Case %s: %s", tc.name, err)
			}

			t.Run("DidNotModifyInput", func(t *testing.T) {
				if err := equalOutput(inputCopy, tc.input, nil, nil); err != nil {
					t.Fatalf("Case %s: Input was modified, err = %s", tc.name, err)
				}
			})

			t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
				if err := dataMap.CheckForOverlappingPoints(); err != nil {
					t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", tc.name, err)
				}
			})

			t.Run("OutputIsCanonical", func(t *testing.T) {
				for i, s := range got {
					if !s.IsCanonical() {
						t.Fatalf("Case %s: output series %d is not canonical: %v", tc.name, i, s)
					}
				}
			})

		})
	}
}

func BenchmarkOffsetToZero10k_1NoNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkOffsetToZero10k_10NoNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkOffsetToZero10k_100NoNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkOffsetToZero10k_1000NoNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkOffsetToZero10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkOffsetToZero10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkOffsetToZero10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkOffsetToZero(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkOffsetToZero(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
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
		f := NewOffsetToZero()
		f.(*FuncOffsetToZero).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
