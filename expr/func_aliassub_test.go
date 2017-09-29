package expr

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestAliasSub(t *testing.T) {
	cases := []struct {
		search  string
		replace string
		in      []string
		out     []string
	}{
		{
			"this",
			"that",
			[]string{"series.name.this.ok"},
			[]string{"series.name.that.ok"},
		},
		{
			`^.*TCP(\d+)`,
			`\1`,
			[]string{"ip-foobar-TCP25", "ip-foobar-TCPfoo"},
			[]string{"25", "ip-foobar-TCPfoo"},
		},
		{
			".*\\.([^\\.]+)\\.metrics_received.*",
			"\\1 in",
			[]string{"metrictank.stats.env.instance.input.pluginname.metrics_received.counter32"},
			[]string{"pluginname in"},
		},
	}
	for i, c := range cases {
		f := NewAliasSub()
		alias := f.(*FuncAliasSub)
		alias.search = regexp.MustCompile(c.search)
		alias.replace = c.replace
		var in []models.Series
		for _, name := range c.in {
			in = append(in, models.Series{
				Target: name,
			})
		}
		alias.in = NewMock(in)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			t.Fatalf("case %d: err should be nil. got %q", i, err)
		}
		if len(got) != len(in) {
			t.Fatalf("case %d: alias output should be same amount of series as input: %d, not %d", i, len(in), len(got))
		}
		for i, o := range c.out {
			g := got[i]
			if o != g.Target {
				t.Fatalf("case %d: expected target %q, got %q", i, o, g.Target)
			}
		}
	}
}

func BenchmarkAliasSub_1(b *testing.B) {
	benchmarkAliasSub(b, 1)
}
func BenchmarkAliasSub_10(b *testing.B) {
	benchmarkAliasSub(b, 10)
}
func BenchmarkAliasSub_100(b *testing.B) {
	benchmarkAliasSub(b, 100)
}
func BenchmarkAliasSub_1000(b *testing.B) {
	benchmarkAliasSub(b, 1000)
}

func benchmarkAliasSub(b *testing.B, numSeries int) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: fmt.Sprintf("metrictank.stats.env.instance.input.plugin%d.metrics_received.counter32", i),
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewAliasSub()
		alias := f.(*FuncAliasSub)
		alias.search = regexp.MustCompile(".*\\.([^\\.]+)\\.metrics_received.*")
		alias.replace = "\\1 in"
		alias.in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
