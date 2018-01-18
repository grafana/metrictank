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
	for i, c := range cases {
		var in []models.Series
		for _, name := range c.in {
			in = append(in, models.Series{
				Target: name,
			})
		}

		{
			f := NewGrep()
			grep := f.(*FuncGrep)
			grep.pattern = regexp.MustCompile(c.pattern)
			grep.in = NewMock(in)
			checkGrepOutput(t, f, i, c.matches)
		}

		{
			f := NewExclude()
			grep := f.(*FuncGrep)
			grep.pattern = regexp.MustCompile(c.pattern)
			grep.in = NewMock(in)
			checkGrepOutput(t, f, i, c.nonmatches)
		}
	}
}

func checkGrepOutput(t *testing.T, f GraphiteFunc, i int, expected []string) {
	got, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %d: err should be nil. got %q", i, err)
	}
	if len(got) != len(expected) {
		t.Fatalf("case %d: expected %d output series, got %d", i, len(expected), len(got))
	}
	for i, o := range expected {
		g := got[i]
		if o != g.Target {
			t.Fatalf("case %d: expected target %q, got %q", i, o, g.Target)
		}
	}
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
