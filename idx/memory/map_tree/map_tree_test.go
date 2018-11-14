package memory

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/raintank/schema"
	. "github.com/smartystreets/goconvey/convey"
)

func getRandomPaths(paths, nodes, nodeLen int) []string {
	const letterBytes = "abcde"
	resMap := make(map[string]struct{})
	builder := strings.Builder{}

	for i := 0; i < paths; i++ {
		builder.Reset()
		for j := 0; j < nodes; j++ {
			if j > 0 {
				builder.WriteString(".")
			}
			for k := 0; k < nodeLen; k++ {
				builder.WriteByte(letterBytes[rand.Intn(len(letterBytes))])
			}
		}
		resMap[builder.String()] = struct{}{}
	}

	res := make([]string, 0, len(resMap))
	for k := range resMap {
		res = append(res, k)
	}

	return res
}

func BenchmarkMapTreeInsert(b *testing.B) {
	randomPaths := getRandomPaths(b.N, 3, 6)

	b.ReportAllocs()
	b.ResetTimer()

	tree := newMapTree()

	mkey, _ := schema.MKeyFromString("1.01234567890123456789012345678901")
	for _, k := range randomPaths {
		tree.insert(strings.Split(k, "."), mkey)
	}
}

func BenchmarkMapTreeSearch(b *testing.B) {
	randomPaths := getRandomPaths(1000000, 3, 3)
	pathCount := len(randomPaths)

	tree := newMapTree()

	mkey, _ := schema.MKeyFromString("1.01234567890123456789012345678901")
	for _, k := range randomPaths {
		tree.insert(strings.Split(k, "."), mkey)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		walker := tree.walker()
		res := walker.get(strings.Split(randomPaths[n%pathCount], "."))
		if len(res) != 1 {
			b.Fatalf("wrong count: %d", len(res))
		}
		walker.reset()
	}
}

func TestMapTreeInsertAndGet(t *testing.T) {
	Convey("When inserting a value into the tree", t, func() {
		tree := newMapTree()
		test1Path := []string{"aaa", "bbb", "ccc"}
		v1, _ := schema.MKeyFromString("1.01234567890123456789012345678901")
		tree.insert(test1Path, v1)
		Convey("We should be able to get it from the same path", func() {
			walker := tree.walker()
			res1 := walker.get(test1Path)
			So(len(res1), ShouldEqual, 1)
			So(res1[0].String(), ShouldEqual, v1.String())
		})
	})

	Convey("When inserting two values into the tree in the same branch", t, func() {
		tree := newMapTree()
		test1Path := []string{"aaa", "bbb", "ccc"}
		v1, _ := schema.MKeyFromString("1.01234567890123456789012345678901")
		v2, _ := schema.MKeyFromString("1.01234567890123456789012345678902")
		tree.insert(test1Path, v1)
		tree.insert(test1Path, v2)
		Convey("We should be able to get both from the same path", func() {
			walker := tree.walker()
			res1 := walker.get(test1Path)
			So(len(res1), ShouldEqual, 2)
			So(res1[0].String(), ShouldEqual, v1.String())
			So(res1[1].String(), ShouldEqual, v2.String())
		})
	})

	Convey("When inserting two values into the tree in different branches", t, func() {
		tree := newMapTree()
		test1Path := []string{"aaa", "bbb", "ccc"}
		test2Path := []string{"aaa", "ccc", "ddd"}
		v1, _ := schema.MKeyFromString("1.01234567890123456789012345678901")
		v2, _ := schema.MKeyFromString("1.01234567890123456789012345678902")
		tree.insert(test1Path, v1)
		tree.insert(test2Path, v2)
		Convey("We should be able to get both from their according path", func() {
			walker := tree.walker()
			res1 := walker.get(test1Path)
			So(len(res1), ShouldEqual, 1)
			So(res1[0].String(), ShouldEqual, v1.String())
			walker.reset()
			res2 := walker.get(test2Path)
			So(len(res2), ShouldEqual, 1)
			So(res2[0].String(), ShouldEqual, v2.String())
		})
	})

	Convey("When inserting three values into the tree in different branches", t, func() {
		tree := newMapTree()
		test1Path := []string{"aaa", "aaa", "ccc"}
		test2Path := []string{"aaa", "aab", "ccc"}
		test3Path := []string{"aaa", "aac", "ccc"}
		v1, _ := schema.MKeyFromString("1.01234567890123456789012345678901")
		v2, _ := schema.MKeyFromString("1.01234567890123456789012345678902")
		v3, _ := schema.MKeyFromString("1.01234567890123456789012345678903")
		tree.insert(test1Path, v1)
		tree.insert(test2Path, v2)
		tree.insert(test3Path, v3)
		Convey("We should be able to get two of them by querying a pattern", func() {
			walker := tree.walker()
			results := walker.get([]string{"aaa", "aa[bc]", "*"})
			So(len(results), ShouldEqual, 2)

			found := make([]bool, 2)
			for _, res := range results {
				if res.String() == v2.String() {
					found[0] = true
				}
				if res.String() == v3.String() {
					found[1] = true
				}
			}
			So(found[0] && found[1], ShouldBeTrue)
		})
	})
}
