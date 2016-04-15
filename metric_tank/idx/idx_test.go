package idx

import (
	"github.com/dgryski/go-trigram"
	"math"
	"sort"
	"testing"
)

func TestGetAddKey(t *testing.T) {
	ix := New()

	// test GetOrAdd
	cases := []struct {
		org int
		key string
		id  MetricID
	}{
		{-1, "abc.def.globallyvisible", 0},
		{1, "abc.def.ghi", 1},
		{1, "abc.def.GHI", 2},
		{1, "abc.def.ghi", 1},  // should already exist -> get same id back
		{2, "abc.def.ghi", 3},  // non unique name, but unique org-key pair -> new docId
		{2, "abc.def.ghij", 4}, // unique org-key combo -> new docId
	}
	for i, c := range cases {
		id := ix.GetOrAdd(c.org, c.key)
		if id != c.id {
			t.Fatalf("case %d: expected id %d - got %d", i, c.id, id)
		}
	}

	// test Get()
	newcases := []struct {
		org int
		key string
		id  MetricID
		ok  bool
	}{
		{2, "abc.def.globallyvisible", 0, true}, // any org should be able to get globally accessible metric
		{2, "abc.def.GHI", 0, false},            // an org should not be able to see other orgs metrics
	}
	// first repeat all cases from above. should all find with Get() as well.
	for i, c := range cases {
		id, ok := ix.Get(c.org, c.key)
		if id != c.id {
			t.Fatalf("case %d: expected id %d - got %d", i, c.id, id)
		}
		if !ok {
			t.Fatalf("case %d: expected ok true - got ok false", i)
		}
	}
	// then try the new cases
	for i, c := range newcases {
		id, ok := ix.Get(c.org, c.key)
		if id != c.id {
			t.Fatalf("case %d: expected id %d - got %d", i, c.id, id)
		}
		if ok != c.ok {
			t.Fatalf("case %d: expected ok %t - got ok %t", i, c.ok, ok)
		}
	}

	// test Key()
	for i, c := range cases {
		key := ix.Key(c.id)
		if key != c.key {
			t.Fatalf("case %d: expected key %s - got %s", i, c.key, key)
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
		_, globs := ix.Match(c.org, c.query)
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
			if globs[j] != c.out[j] {
				fail()
			}
		}
	}
	// now we make sure wildcards work, and you can see your own orgs stuff + org -1, but no other orgs of course
	ix.GetOrAdd(-1, "abc.def.globallyvisible") // id 0
	ix.GetOrAdd(1, "abc.def.ghi")              // id 1
	ix.GetOrAdd(1, "abc.def.GHI")              // id 2
	ix.GetOrAdd(2, "abc.def.ghi")              // id 3
	ix.GetOrAdd(2, "abc.def.ghij")             // id 4
	ix.AddRef(-1, 0)
	ix.AddRef(1, 1)
	ix.AddRef(1, 2)
	ix.AddRef(2, 3)
	ix.AddRef(2, 4)
	cases = []struct {
		org   int
		query string
		out   []Glob
	}{
		// no stars
		{1, "abc.def.ghi", []Glob{{1, "abc.def.ghi", true}}},

		// no star try to get other org specifically
		{1, "abc.def.ghij", []Glob{}},
		{2, "abc.def.GHI", []Glob{}},

		// prefix star
		{1, "*.def.ghi", []Glob{{1, "abc.def.ghi", true}}},

		// postfix and mid stars
		{1, "abc.def.*", []Glob{{0, "abc.def.globallyvisible", true}, {1, "abc.def.ghi", true}, {2, "abc.def.GHI", true}}},
		{1, "abc.def.g*", []Glob{{0, "abc.def.globallyvisible", true}, {1, "abc.def.ghi", true}}},
		{1, "abc.def.gh*", []Glob{{1, "abc.def.ghi", true}}},
		{1, "abc.*e*.*", []Glob{{0, "abc.def.globallyvisible", true}, {1, "abc.def.ghi", true}, {2, "abc.def.GHI", true}}},
		{1, "abc.d*.*", []Glob{{0, "abc.def.globallyvisible", true}, {1, "abc.def.ghi", true}, {2, "abc.def.GHI", true}}},
		{1, "abc.*.*", []Glob{{0, "abc.def.globallyvisible", true}, {1, "abc.def.ghi", true}, {2, "abc.def.GHI", true}}},
		{1, "abc.*.ghi", []Glob{{1, "abc.def.ghi", true}}},
		{1, "*.*.ghi", []Glob{{1, "abc.def.ghi", true}}},
		{1, "*.*.ghij", []Glob{}},
		{1, "bc.*.*", []Glob{}},

		// all stars
		{1, "*.*.*", []Glob{{0, "abc.def.globallyvisible", true}, {1, "abc.def.ghi", true}, {2, "abc.def.GHI", true}}},
	}
	for i, c := range cases {
		_, globs := ix.Match(c.org, c.query)
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

// TODO unit tests for trigram pruning don't do org trigrams
