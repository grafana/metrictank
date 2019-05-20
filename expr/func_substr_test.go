package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
)

func getNewSubstr(in []models.Series, start, stop int64) *FuncSubstr {
	f := NewSubstr()
	s := f.(*FuncSubstr)
	s.in = NewMock(in)
	s.start = start
	s.stop = stop
	return s
}

func TestSubstrNoArgs(t *testing.T) {
	f := getNewSubstr(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a.b.c.d.*",
				Target:     "a.b.c.d.e",
				Datapoints: getCopy(a),
			},
		},
		0,
		0,
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "a.b.c.d.*",
			Target:     "a.b.c.d.e",
			Datapoints: getCopy(a),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestSubstrStart(t *testing.T) {
	f := getNewSubstr(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a.b.c.d.*",
				Target:     "a.b.c.d.e",
				Datapoints: getCopy(a),
			},
		},
		2,
		0,
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "a.b.c.d.*",
			Target:     "c.d.e",
			Datapoints: getCopy(a),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestSubstrStartStop(t *testing.T) {
	f := getNewSubstr(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a.b.c.d.*",
				Target:     "a.b.c.d.e",
				Datapoints: getCopy(a),
			},
		},
		2,
		4,
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "a.b.c.d.*",
			Target:     "c.d",
			Datapoints: getCopy(a),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestSubstrWithFuncs(t *testing.T) {
	f := getNewSubstr(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "foo(bar(a.b.c.d.*,foo))",
				Target:     "foo(bar(a.b.c.d.e,foo))",
				Datapoints: getCopy(a),
			},
		},
		2,
		4,
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "foo(bar(a.b.c.d.*,foo))",
			Target:     "c.d",
			Datapoints: getCopy(a),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSubstr10k_1NoNulls(b *testing.B) {
	benchmarkSubstr(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSubstr10k_10NoNulls(b *testing.B) {
	benchmarkSubstr(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSubstr10k_100NoNulls(b *testing.B) {
	benchmarkSubstr(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSubstr10k_1000NoNulls(b *testing.B) {
	benchmarkSubstr(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkSubstr10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSubstr(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSubstr10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSubstr(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSubstr10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSubstr(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSubstr10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkSubstr(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSubstr10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkSubstr(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSubstr10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkSubstr(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSubstr10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkSubstr(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkSubstr10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkSubstr(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkSubstr(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewSubstr()
		f.(*FuncSubstr).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
