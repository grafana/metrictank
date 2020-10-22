package memory

import (
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/grafana/metrictank/schema"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTreeFromPath(t *testing.T) {
	type testCase struct {
		paths []string
		tree  *bareTree
	}
	testCases := []testCase{
		{
			paths: []string{"foo.bar.baz"},
			tree: &bareTree{
				Items: map[string]*Node{
					"": {
						Path:     "",
						Children: []string{"foo"},
					},
					"foo": {
						Path:     "foo",
						Children: []string{"bar"},
					},
					"foo.bar": {
						Path:     "foo.bar",
						Children: []string{"baz"},
					},
					"foo.bar.baz": {
						Path: "foo.bar.baz",
						Defs: []schema.MKey{},
					},
				},
			},
		},
		{
			paths: []string{"foo.bar.baz", "foo.abc", "a"},
			tree: &bareTree{
				Items: map[string]*Node{
					"": {
						Path:     "",
						Children: []string{"foo", "a"},
					},
					"a": {
						Path: "a",
						Defs: []schema.MKey{},
					},
					"foo": {
						Path:     "foo",
						Children: []string{"bar", "abc"},
					},
					"foo.abc": {
						Path: "foo.abc",
						Defs: []schema.MKey{},
					},
					"foo.bar": {
						Path:     "foo.bar",
						Children: []string{"baz"},
					},
					"foo.bar.baz": {
						Path: "foo.bar.baz",
						Defs: []schema.MKey{},
					},
				},
			},
		},
	}
	for i, c := range testCases {
		tree := newBareTree()
		for _, path := range c.paths {
			tree.add(path)
		}
		if !reflect.DeepEqual(tree, c.tree) {
			t.Errorf("TestTreeFromPath case %d\nexpected:\n%s\ngot:\n%s", i, spew.Sdump(c.tree), spew.Sdump(tree))
		}
	}
}

func TestFindCache(t *testing.T) {
	Convey("when findCache is empty", t, func() {
		c := NewFindCache(10, 5, 2, 100*time.Millisecond, time.Second*2)
		Convey("0 results should be returned", func() {
			result, ok := c.Get(1, "foo.bar.*", 0)
			So(ok, ShouldBeFalse)
			So(result.nodes, ShouldHaveLength, 0)
		})
		Convey("when adding entries to the cache", func() {
			pattern := "foo.bar.*"
			tree := newBareTree()
			tree.add("foo.bar.foo")
			results, err := find((*Tree)(tree), pattern, 0)
			So(err, ShouldBeNil)
			So(results, ShouldHaveLength, 1)
			c.Add(1, "foo.bar.*", 0, results, nil)
			So(c.cache[1].Len(), ShouldEqual, 1)
			Convey("when getting cached pattern", func() {
				result, ok := c.Get(1, "foo.bar.*", 0)
				So(ok, ShouldBeTrue)
				So(result.nodes, ShouldHaveLength, 1)
				Convey("After invalidating path that matches pattern", func() {
					c.InvalidateFor(1, "foo.bar.baz")
					time.Sleep(time.Second) // make sure we reach invalidateMaxWait
					So(c.cache[1].Len(), ShouldEqual, 0)
				})
				Convey("After invalidating path that doesn't match cached pattern", func() {
					c.InvalidateFor(1, "foo.foo.baz")
					So(c.cache[1].Len(), ShouldEqual, 1)
				})
			})
			Convey("when findCache invalidation falls behind", func() {
				c.Add(1, "foo.{a,b,c}*.*", 0, results, nil)
				c.Add(1, "foo.{a,b,e}*.*", 0, results, nil)
				c.Add(1, "foo.{a,b,f}*.*", 0, results, nil)
				c.triggerBackoff()
				c.InvalidateFor(1, "foo.baz.foo.a.b.c.d.e.f.g.h")

				So(len(c.cache), ShouldEqual, 0)
				Convey("when adding to cache in backoff", func() {
					c.Add(1, "foo.*.*", 0, results, nil)
					So(len(c.cache), ShouldEqual, 0)
					result, ok := c.Get(1, "foo.*.*", 0)
					So(ok, ShouldBeFalse)
					So(result.nodes, ShouldHaveLength, 0)
				})
				Convey("when adding to cache after backoff time", func() {
					time.Sleep(time.Millisecond * 2500)
					c.Add(1, "foo.*.*", 0, results, nil)
					So(len(c.cache), ShouldEqual, 1)
					result, ok := c.Get(1, "foo.*.*", 0)
					So(ok, ShouldBeTrue)
					So(result.nodes, ShouldHaveLength, 1)
				})
			})
		})
	})

}

func BenchmarkTreeFromPath(b *testing.B) {
	numPaths := 1000
	paths := getSeriesNames(10, numPaths, "benchmark")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := i % numPaths
		tree := newBareTree()
		tree.add(paths[p])
	}
}
