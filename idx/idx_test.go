package idx

import (
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/dgryski/go-trigram"
	"gopkg.in/raintank/schema.v1"
)

func TestGetAddKey(t *testing.T) {
	ix := New()

	type add struct {
		org  int
		name string
		id   DocID
	}
	adds := []add{
		{-1, "abc.def.globallyvisible", 0},
		{1, "abc.def.ghi", 1},
		{1, "abc.def.GHI", 2},
		{2, "abc.def.ghi", 3},
		{2, "abc.def.ghij", 4},
	}
	for i, a := range adds {
		def := schema.MetricDefinition{
			OrgId: a.org,
			Name:  a.name,
		}
		def.SetId()
		id := ix.Add(def)
		if id != a.id {
			t.Fatalf("case %d: expected id %d - got %d", i, a.id, id)
		}
	}

	defs := func(m ...add) []schema.MetricDefinition {
		out := make([]schema.MetricDefinition, len(m))
		for i := range m {
			out[i] = schema.MetricDefinition{
				OrgId: m[i].org,
				Name:  m[i].name,
			}
			out[i].SetId()
		}
		return out
	}

	// test Get()
	gets := []struct {
		org   int
		query string
		resp  []schema.MetricDefinition
	}{
		{-1, "abc.def.globallyvisible", defs(adds[0])},
		{1, "abc.def.ghi", defs(adds[1])},
		{1, "abc.def.GHI", defs(adds[2])},
		{2, "abc.def.ghi", defs(adds[3])},
		{2, "abc.def.ghij", defs(adds[4])},
		{2, "abc.def.globallyvisible", defs(adds[0])}, // any org should be able to get globally accessible metric
		{2, "abc.def.GHI", defs()},                    // an org should not be able to see other orgs metrics
	}
	for i, g := range gets {
		_, _, defs := ix.Match(g.org, g.query)
		if len(defs) != len(g.resp) {
			t.Fatalf("case %d: expected %d matches - got %d: %q", i, len(g.resp), len(defs), defs)
		}
		for j := range defs {
			if !reflect.DeepEqual(*defs[j], g.resp[j]) {
				t.Fatalf("case %d: expected %v - got %v", i, g.resp[j], defs[j])
			}
		}
	}
}

func TestOrgToTrigram(t *testing.T) {
	cases := []struct {
		org int
		t   trigram.T
	}{
		{-1, 1 << 31},
		{0, 1<<31 + 1},
		{1, 1<<31 + 2},
		{5, 1<<31 + 6},
		{2147483645, trigram.T(math.MaxUint32 - 1)}, // TODO test the panic of this+1
	}
	for i, c := range cases {
		tri := orgToTrigram(c.org)
		if tri != c.t {
			t.Fatalf("case %d: expected trigram %d - got %d", i, c.t, tri)
		}
	}
}

type globByName []Glob

func (g globByName) Len() int           { return len(g) }
func (g globByName) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g globByName) Less(i, j int) bool { return g[i].Metric < g[j].Metric }

func TestMatch(t *testing.T) {
	ix := New()
	cases := []struct {
		org   int
		query string
		out   []Glob
	}{
		{10, "abc.def.ghi", []Glob{}},
		{10, "abc.def.*", []Glob{}},
		{10, "abc.*.*", []Glob{}},
		{10, "abc.*.ghi", []Glob{}},
	}
	for i, c := range cases {
		_, globs, _ := ix.Match(c.org, c.query)
		fail := func() {
			for j := 0; j < len(c.out); j++ {
				t.Logf("expected glob %d -> %s", j, c.out[j])
			}
			for j := 0; j < len(globs); j++ {
				t.Logf("got glob %d -> %s", j, globs[j])
			}
			t.Fatalf("case %d: glob mismatch. see above for details", i)
		}
		if len(c.out) != len(globs) {
			fail()
		}
		for j := 0; j < len(globs); j++ {
			if !reflect.DeepEqual(globs[j], c.out[j]) {
				fail()
			}
		}
	}
	// now we make sure wildcards work, and you can see your own orgs stuff + org -1, but no other orgs of course
	def := func(org int, name string) schema.MetricDefinition {
		s := schema.MetricDefinition{
			OrgId: org,
			Name:  name,
		}
		s.SetId()
		return s
	}
	ix.Add(def(-1, "abc.def.globallyvisible")) // id 0
	ix.Add(def(1, "abc.def.ghi"))              // id 1
	ix.Add(def(1, "abc.def.GHI"))              // id 2
	ix.Add(def(2, "abc.def.ghi"))              // id 3
	ix.Add(def(2, "abc.def.ghij"))             // id 4
	cases = []struct {
		org   int
		query string
		out   []Glob
	}{
		// no stars
		{1, "abc.def.ghi", []Glob{{"abc.def.ghi", true}}},

		// no star try to get other org specifically
		{1, "abc.def.ghij", []Glob{}},
		{2, "abc.def.GHI", []Glob{}},

		// prefix star
		{1, "*.def.ghi", []Glob{{"abc.def.ghi", true}}},

		// postfix and mid stars
		{1, "abc.def.*", []Glob{{"abc.def.globallyvisible", true}, {"abc.def.ghi", true}, {"abc.def.GHI", true}}},
		{1, "abc.def.g*", []Glob{{"abc.def.globallyvisible", true}, {"abc.def.ghi", true}}},
		{1, "abc.def.gh*", []Glob{{"abc.def.ghi", true}}},
		{1, "abc.*e*.*", []Glob{{"abc.def.globallyvisible", true}, {"abc.def.ghi", true}, {"abc.def.GHI", true}}},
		{1, "abc.d*.*", []Glob{{"abc.def.globallyvisible", true}, {"abc.def.ghi", true}, {"abc.def.GHI", true}}},
		{1, "abc.*.*", []Glob{{"abc.def.globallyvisible", true}, {"abc.def.ghi", true}, {"abc.def.GHI", true}}},
		{1, "abc.*.ghi", []Glob{{"abc.def.ghi", true}}},
		{1, "*.*.ghi", []Glob{{"abc.def.ghi", true}}},
		{1, "*.*.ghij", []Glob{}},
		{1, "bc.*.*", []Glob{}},

		// curly braces
		{1, "abc.*.g{loballyvisible,hi}", []Glob{{"abc.def.globallyvisible", true}, {"abc.def.ghi", true}}},
		{1, "abc.*.{GHI,ghi}", []Glob{{"abc.def.ghi", true}, {"abc.def.GHI", true}}},
		{1, "abc.*.{G,}HI", []Glob{{"abc.def.GHI", true}}},

		// all stars
		{1, "*.*.*", []Glob{{"abc.def.globallyvisible", true}, {"abc.def.ghi", true}, {"abc.def.GHI", true}}},
	}
	for i, c := range cases {
		_, globs, _ := ix.Match(c.org, c.query)
		sort.Sort(globByName(globs))
		sort.Sort(globByName(c.out))
		fail := func() {
			t.Logf("case %d: org: %d - query: %q . globs mismatch", i, c.org, c.query)
			t.Log("expected globs:")
			for j := 0; j < len(c.out); j++ {
				t.Logf("[%d] = %s", j, c.out[j])
			}
			t.Log("got globs:")
			for j := 0; j < len(globs); j++ {
				t.Logf("[%d] = %s", j, globs[j])
			}
			t.Fatalf("case %d: glob mismatch. see above for details", i)
		}
		if len(c.out) != len(globs) {
			fail()
		}
		for j := 0; j < len(globs); j++ {
			if globs[j] != c.out[j] {
				fail()
			}
		}
	}
}

func TestExpandQueries(t *testing.T) {
	cases := []struct {
		in  string
		out []string
	}{
		{
			"foo.bar",
			[]string{"foo.bar"},
		},
		{
			"foo.b{a,b,c}r",
			[]string{"foo.bar", "foo.bbr", "foo.bcr"},
		},
		{
			"foo.bar.{abc,}",
			[]string{"foo.bar.abc", "foo.bar."},
		},
		{
			"foo.bar.{abc",
			[]string{"foo.bar.{abc"}, // this would be better if we returned error to user
		},
	}
	for i, c := range cases {
		ret := expandQueries(c.in)
		if len(ret) != len(c.out) {
			t.Fatalf("case %d: output mismatch: expected %v got %v", i, c.out, ret)
		}
		for f, expanded := range ret {
			if expanded != c.out[f] {
				t.Fatalf("case %d elem %d expected %q got %q", i, f, c.out[f], expanded)
			}
		}
	}
}

// TODO unit tests for trigram pruning don't do org trigrams
