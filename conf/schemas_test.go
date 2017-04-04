package conf

import (
	. "github.com/smartystreets/goconvey/convey"
	"regexp"
	"testing"
)

func schemasForTest() Schemas {
	return NewSchemas([]Schema{
		{
			Name:    "a",
			Pattern: regexp.MustCompile("^a\\..*"),
			Retentions: []Retention{
				NewRetentionMT(10, 3600, 60*10, 0, true),
				NewRetentionMT(3600, 86400, 60*60*6, 0, true),
			},
		},
		{
			Name:    "b",
			Pattern: regexp.MustCompile("^b\\..*"),
			Retentions: []Retention{
				NewRetentionMT(1, 60, 60*10, 0, true),
				NewRetentionMT(30, 120, 60*30, 0, true),
				NewRetentionMT(600, 86400, 60*60*6, 0, true),
			},
		},
		{
			Name:    "default",
			Pattern: regexp.MustCompile(".*"),
			Retentions: []Retention{
				NewRetentionMT(1, 60, 60*10, 0, true),
				NewRetentionMT(60, 3600, 60*60*2, 0, true),
				NewRetentionMT(600, 86400, 60*60*6, 0, true),
				NewRetentionMT(3600, 86400*7, 60*60*6, 0, true),
			},
		},
	})
}

func TestMatch(t *testing.T) {
	schemas := schemasForTest()
	Convey("When matching against first schema", t, func() {
		Convey("When metric has 1s raw interval", func() {
			id, schema := schemas.Match("a.foo", 1)
			So(id, ShouldEqual, 0)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 10)
		})
		Convey("When metric has 10s raw interval", func() {
			id, schema := schemas.Match("a.foo", 10)
			So(id, ShouldEqual, 0)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 10)
		})
		Convey("When metric has 30s raw interval", func() {
			id, schema := schemas.Match("a.foo", 30)
			So(id, ShouldEqual, 0)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 10)
		})
		Convey("When metric has 2h raw interval", func() {
			id, schema := schemas.Match("a.foo", 7200)
			So(id, ShouldEqual, 1)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 3600)
		})
	})
	Convey("When matching against second schema", t, func() {
		Convey("When metric has 1s raw interval", func() {
			id, schema := schemas.Match("b.foo", 1)
			So(id, ShouldEqual, 2)
			So(schema.Name, ShouldEqual, "b")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 1)
		})
		Convey("When metric has 10s raw interval", func() {
			id, schema := schemas.Match("b.foo", 10)
			So(id, ShouldEqual, 2)
			So(schema.Name, ShouldEqual, "b")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 1)
		})
		Convey("When metric has 30s raw interval", func() {
			id, schema := schemas.Match("b.foo", 30)
			So(id, ShouldEqual, 3)
			So(schema.Name, ShouldEqual, "b")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 30)
		})
		Convey("When metric has 2h raw interval", func() {
			id, schema := schemas.Match("b.foo", 7200)
			So(id, ShouldEqual, 4)
			So(schema.Name, ShouldEqual, "b")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 600)
		})
	})
	Convey("When matching against default schema", t, func() {
		Convey("When metric has 1s raw interval", func() {
			id, schema := schemas.Match("c.foo", 1)
			So(id, ShouldEqual, 5)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 1)
		})
		Convey("When metric has 10s raw interval", func() {
			id, schema := schemas.Match("c.foo", 10)
			So(id, ShouldEqual, 5)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 1)
		})
		Convey("When metric has 30s raw interval", func() {
			id, schema := schemas.Match("c.foo", 60)
			So(id, ShouldEqual, 6)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 60)
		})
		Convey("When metric has 2h raw interval", func() {
			id, schema := schemas.Match("c.foo", 7200)
			So(id, ShouldEqual, 8)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 3600)
		})
	})
}

func TestDefaultSchema(t *testing.T) {
	schemas := NewSchemas([]Schema{
		{
			Name:    "a",
			Pattern: regexp.MustCompile("^a\\..*"),
			Retentions: []Retention{
				NewRetentionMT(10, 3600, 60*10, 0, true),
				NewRetentionMT(3600, 86400, 60*60*6, 0, true),
			},
		},
	})

	Convey("When matching against first schema", t, func() {
		Convey("When metric has 1s raw interval", func() {
			id, schema := schemas.Match("a.foo", 1)
			So(id, ShouldEqual, 0)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 10)
		})
	})
	Convey("When series doesnt match any schema", t, func() {
		Convey("When metric has 10s raw interval", func() {
			id, schema := schemas.Match("d.foo", 10)
			So(id, ShouldEqual, 2)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions[0].SecondsPerPoint, ShouldEqual, 1)
		})
	})
}

func TestTTLs(t *testing.T) {
	schemas := schemasForTest()
	Convey("When getting list of TTLS", t, func() {
		ttls := schemas.TTLs()
		So(len(ttls), ShouldEqual, 5)
	})
}
func TestMaxChunkSpan(t *testing.T) {
	schemas := schemasForTest()
	Convey("When getting maxChunkSpan", t, func() {
		max := schemas.MaxChunkSpan()
		So(max, ShouldEqual, 60*60*6)
	})
}
