package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/test"
)

func TestSubstrNoArgs(t *testing.T) {
	input := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "a.b.c.d.*",
			Target:     "a.b.c.d.e",
			Datapoints: getCopy(a),
		},
	}
	exp := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "a.b.c.d.*",
			Target:     "a.b.c.d.e",
			Datapoints: getCopy(a),
		},
	}
	testSubstr("TestSubstrNoArgs", input, exp, 0, 0, t)
}

func TestSubstrStart(t *testing.T) {
	input := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "a.b.c.d.*",
			Target:     "a.b.c.d.e",
			Datapoints: getCopy(a),
		},
	}
	exp := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "a.b.c.d.*",
			Target:     "c.d.e",
			Datapoints: getCopy(a),
		},
	}
	testSubstr("TestSubstrStart", input, exp, 2, 0, t)
}

func TestSubstrStartStop(t *testing.T) {
	input := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "a.b.c.d.*",
			Target:     "a.b.c.d.e",
			Datapoints: getCopy(a),
		},
	}
	exp := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "a.b.c.d.*",
			Target:     "c.d",
			Datapoints: getCopy(a),
		},
	}
	testSubstr("TestSubstrStartStop", input, exp, 2, 4, t)
}

func TestSubstrWithFuncs(t *testing.T) {
	input := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "foo(bar(a.b.c.d.*,foo))",
			Target:     "foo(bar(a.b.c.d.e,foo))",
			Datapoints: getCopy(a),
		},
	}
	exp := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "foo(bar(a.b.c.d.*,foo))",
			Target:     "c.d",
			Datapoints: getCopy(a),
		},
	}
	testSubstr("TestSubstrWithFuncs", input, exp, 2, 4, t)
}

func testSubstr(name string, in []models.Series, out []models.Series, start, stop int64, t *testing.T) {
	f := NewSubstr()
	substr := f.(*FuncSubstr)
	substr.in = NewMock(in)
	substr.start = start
	substr.stop = stop

	inputCopy := models.SeriesCopy(in) // to later verify that it is unchanged

	dataMap := initDataMap(in)

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Case %s: Input was modified, err = %s", name, err)
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", name, err)
		}
	})
	t.Run("OutputIsCanonical", func(t *testing.T) {
		for i, s := range got {
			if !s.IsCanonical() {
				t.Fatalf("Case %s: output series %d is not canonical: %v", name, i, s)
			}
		}
	})
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
func benchmarkSubstr(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
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
		f := NewSubstr()
		f.(*FuncSubstr).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
