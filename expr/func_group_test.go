package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
)

func getNewGroup(in [][]models.Series) *FuncGroup {
	f := NewGroup()
	s := f.(*FuncGroup)
	for i := range in {
		s.in = append(s.in, NewMock(in[i]))
	}
	return s
}

func TestGroup(t *testing.T) {
	f := getNewGroup([][]models.Series{
		{
			{
				Interval:   10,
				QueryPatt:  "a",
				Target:     "a",
				Datapoints: getCopy(a),
			},
		},
		{
			{
				Interval:   10,
				QueryPatt:  "b",
				Target:     "b",
				Datapoints: getCopy(b),
			},
		},
		{
			{
				Interval:   10,
				QueryPatt:  "abc",
				Target:     "abc",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "abc",
				Target:     "abc",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "abc",
				Target:     "abc",
				Datapoints: getCopy(c),
			},
		},
	},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "a",
			Target:     "a",
			Datapoints: getCopy(a),
		},
		{
			Interval:   10,
			QueryPatt:  "b",
			Target:     "b",
			Datapoints: getCopy(b),
		},
		{
			Interval:   10,
			QueryPatt:  "abc",
			Target:     "abc",
			Datapoints: getCopy(a),
		},
		{
			Interval:   10,
			QueryPatt:  "abc",
			Target:     "abc",
			Datapoints: getCopy(b),
		},
		{
			Interval:   10,
			QueryPatt:  "abc",
			Target:     "abc",
			Datapoints: getCopy(c),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}
func BenchmarkGroup10k_1NoNulls(b *testing.B) {
	benchmarkGroup(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkGroup10k_10NoNulls(b *testing.B) {
	benchmarkGroup(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkGroup10k_100NoNulls(b *testing.B) {
	benchmarkGroup(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkGroup10k_1000NoNulls(b *testing.B) {
	benchmarkGroup(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkGroup10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkGroup10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkGroup(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkGroup(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewGroup()
		f.(*FuncGroup).in = append(f.(*FuncGroup).in, NewMock(input))
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
