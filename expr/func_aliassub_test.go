package expr

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestAliasSubZero(t *testing.T) {
	testAliasSub("n", "z", []string{}, []string{}, t)

}

func TestAliasSub(t *testing.T) {
	testAliasSub("this", "that", []string{"series.name.this.ok"}, []string{"series.name.that.ok"}, t)
	testAliasSub(`^.*TCP(\d+)`, `\1`, []string{"ip-foobar-TCP25", "ip-foobar-TCPfoo"}, []string{"25", "ip-foobar-TCPfoo"}, t)
	testAliasSub(".*\\.([^\\.]+)\\.metrics_received.*", "\\1 in", []string{"metrictank.stats.env.instance.input.pluginname.metrics_received.counter32"}, []string{"pluginname in"}, t)
	testAliasSub(".*host=([^;]+)(;.*)?", "\\1", []string{"foo.bar.baz;a=b;host=ab1"}, []string{"ab1"}, t)
}

func testAliasSub(search, replace string, inStr, outStr []string, t *testing.T) {
	var in, out []models.Series
	for i := range inStr {
		in = append(in, getSeriesNamed(inStr[i], a))
		out = append(out, getSeriesNamed(outStr[i], a))
	}

	f := NewAliasSub()
	alias := f.(*FuncAliasSub)
	alias.search = regexp.MustCompile(search)
	alias.replace = replace
	alias.in = NewMock(in)

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

	dataMap := initDataMap(in)

	name := search + "->" + replace
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
