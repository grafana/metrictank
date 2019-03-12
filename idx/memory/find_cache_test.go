package memory

import (
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTreeFromPath(t *testing.T) {
	path := "foo.bar.baz"
	Convey("when treeFromPath is given a path", t, func() {
		tree := treeFromPath(path)
		So(len(tree.Items), ShouldEqual, 4)
		So(tree.Items[""], ShouldNotBeNil)
		So(tree.Items[""].Leaf(), ShouldBeFalse)
		So(tree.Items["foo"], ShouldNotBeNil)
		So(tree.Items["foo"].Leaf(), ShouldBeFalse)
		So(tree.Items["foo.bar"], ShouldNotBeNil)
		So(tree.Items["foo.bar"].Leaf(), ShouldBeFalse)
		So(tree.Items["foo.bar.baz"], ShouldNotBeNil)
		So(tree.Items["foo.bar.baz"].Leaf(), ShouldBeTrue)
	})
}

func TestFindCache(t *testing.T) {
	Convey("when findCache is empty", t, func() {
		c := NewFindCache(10, 1, time.Second*2)
		Convey("0 results should be returned", func() {
			result, ok := c.Get(1, "foo.bar.*")
			So(ok, ShouldBeFalse)
			So(result, ShouldHaveLength, 0)
		})
		Convey("when adding entries to the cache", func() {
			pattern := "foo.bar.*"
			results, err := find(treeFromPath("foo.bar.foo"), pattern)
			So(err, ShouldBeNil)
			So(results, ShouldHaveLength, 1)
			c.Add(1, "foo.bar.*", results)
			So(c.cache[1].Len(), ShouldEqual, 1)
			Convey("when getting cached pattern", func() {
				result, ok := c.Get(1, "foo.bar.*")
				So(ok, ShouldBeTrue)
				So(result, ShouldHaveLength, 1)
				Convey("After invalidating path that matches pattern", func() {
					c.InvalidateFor(1, "foo.bar.baz")
					So(c.cache[1].Len(), ShouldEqual, 0)
				})
				Convey("After invalidating path that doesnt matche cached pattern", func() {
					c.InvalidateFor(1, "foo.foo.baz")
					So(c.cache[1].Len(), ShouldEqual, 1)
				})
			})
			Convey("when findCache invalidation falls behind", func() {
				var wg sync.WaitGroup
				wg.Add(10)
				for i := 0; i < 10; i++ {
					go func() {
						c.InvalidateFor(1, "foo.baz.foo.a.b.c.d.e.f.g.h")
						wg.Done()
					}()
				}
				wg.Wait()

				So(len(c.cache), ShouldEqual, 0)
				Convey("when adding to cache in backoff", func() {
					c.Add(1, "foo.*.*", results)
					So(len(c.cache), ShouldEqual, 0)
					result, ok := c.Get(1, "foo.*.*")
					So(ok, ShouldBeFalse)
					So(result, ShouldHaveLength, 0)
				})
				Convey("when adding to cache after backoff time", func() {
					time.Sleep(time.Millisecond * 2500)
					c.Add(1, "foo.*.*", results)
					So(len(c.cache), ShouldEqual, 1)
					result, ok := c.Get(1, "foo.*.*")
					So(ok, ShouldBeTrue)
					So(result, ShouldHaveLength, 1)
				})
			})
		})
	})

}
