package expr

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestGrep(t *testing.T) {
	cases := []struct {
		pattern    string
		in         []string
		matches    []string
		nonmatches []string
	}{
		{
			"this",
			[]string{"series.name.this.ok"},
			[]string{"series.name.this.ok"},
			[]string{},
		},
		{
			`cpu\d`,
			[]string{"series.cpu1.ok", "series.cpu2.ok", "series.cpu.notok", "series.cpu3.ok"},
			[]string{"series.cpu1.ok", "series.cpu2.ok", "series.cpu3.ok"},
			[]string{"series.cpu.notok"},
		},
		{
			`cpu[02468]`,
			[]string{"series.cpu1.ok", "series.cpu2.ok", "series.cpu.notok", "series.cpu3.ok"},
			[]string{"series.cpu2.ok"},
			[]string{"series.cpu1.ok", "series.cpu.notok", "series.cpu3.ok"},
		},
	}
	for _, c := range cases {
		testGrep(c.pattern, false, c.in, c.matches, t)
		testGrep(c.pattern, true, c.in, c.nonmatches, t)
	}
}

func makeGrepOrExclude(in []models.Series, pattern string, exclude bool) GraphiteFunc {
	var f GraphiteFunc
	if exclude {
		f = NewExclude()
	} else {
		f = NewGrep()
	}

	grep := f.(*FuncGrep)
	grep.pattern = regexp.MustCompile(pattern)
	grep.in = NewMock(in)
	return f
}

func testGrep(pattern string, exclude bool, inStr, outStr []string, t *testing.T) {
	in := make([]models.Series, 0, len(inStr))
	for _, s := range inStr {
		in = append(in, getSeriesNamed(s, a))
	}

	out := make([]models.Series, 0, len(outStr))
	for _, s := range outStr {
		out = append(out, getSeriesNamed(s, a))
	}
	f := makeGrepOrExclude(in, pattern, exclude)

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

	dataMap := DataMap(make(map[Req][]models.Series))

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Input was modified, err = %s", err)
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Point slices in datamap overlap, err = %s", err)
		}
	})
}

func BenchmarkGrep_1(b *testing.B) {
	benchmarkGrep(b, 1)
}
func BenchmarkGrep_10(b *testing.B) {
	benchmarkGrep(b, 10)
}
func BenchmarkGrep_100(b *testing.B) {
	benchmarkGrep(b, 100)
}
func BenchmarkGrep_1000(b *testing.B) {
	benchmarkGrep(b, 1000)
}
func BenchmarkGrep_100000(b *testing.B) {
	benchmarkGrep(b, 100000)
}

func benchmarkGrep(b *testing.B, numSeries int) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: fmt.Sprintf("metrictank.stats.env.instance.input.plugin%d.metrics_received.counter32", i),
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewGrep()
		grep := f.(*FuncGrep)
		grep.pattern = regexp.MustCompile("input.plugin[0246].metrics")
		grep.in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
