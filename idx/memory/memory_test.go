package memory

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	//"github.com/raintank/metrictank/idx"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/raintank/schema.v1"
)

func getSeriesNames(depth, count int, prefix string) []string {
	series := make([]string, count)
	for i := 0; i < count; i++ {
		ns := make([]string, depth)
		for j := 0; j < depth; j++ {
			ns[j] = getRandomString(4)
		}
		series[i] = prefix + "." + strings.Join(ns, ".")
	}
	return series
}

// source: https://github.com/gogits/gogs/blob/9ee80e3e5426821f03a4e99fad34418f5c736413/modules/base/tool.go#L58
func getRandomString(n int, alphabets ...byte) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		if len(alphabets) == 0 {
			bytes[i] = alphanum[b%byte(len(alphanum))]
		} else {
			bytes[i] = alphabets[b%byte(len(alphabets))]
		}
	}
	return string(bytes)
}

func getMetricData(orgId, depth, count, interval int, prefix string) []*schema.MetricData {
	data := make([]*schema.MetricData, count)
	series := getSeriesNames(depth, count, prefix)
	for i, s := range series {

		data[i] = &schema.MetricData{
			Name:     s,
			Metric:   s,
			OrgId:    orgId,
			Interval: interval,
		}
		data[i].SetId()
	}
	return data
}

func TestGetAddKey(t *testing.T) {
	ix := New()
	ix.Init()

	publicSeries := getMetricData(-1, 2, 5, 10, "metric.public")
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1")
	org2Series := getMetricData(2, 2, 5, 10, "metric.org2")

	for _, series := range [][]*schema.MetricData{publicSeries, org1Series, org2Series} {
		orgId := series[0].OrgId
		Convey(fmt.Sprintf("When indexing metrics for orgId %d", orgId), t, func() {
			for _, s := range series {
				ix.AddOrUpdate(s, 1)
			}
			Convey(fmt.Sprintf("Then listing metrics for OrgId %d", orgId), func() {
				defs := ix.List(orgId)
				numSeries := len(series)
				if orgId != -1 {
					numSeries += 5
				}
				So(defs, ShouldHaveLength, numSeries)

			})
		})
	}

	Convey("When adding metricDefs with the same series name as existing metricDefs", t, func() {
		for _, series := range org1Series {
			series.Interval = 60
			series.SetId()
			ix.AddOrUpdate(series, 1)
		}
		Convey("then listing metrics", func() {
			defs := ix.List(1)
			So(defs, ShouldHaveLength, 15)
		})
	})
}

func TestFind(t *testing.T) {
	ix := New()
	ix.Init()
	for _, s := range getMetricData(-1, 2, 5, 10, "metric.demo") {
		s.Time = 10 * 86400
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range getMetricData(1, 2, 5, 10, "metric.demo") {
		s.Time = 10 * 86400
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range getMetricData(1, 1, 5, 10, "foo.demo") {
		s.Time = 1 * 86400
		ix.AddOrUpdate(s, 1)
		s.Time = 2 * 86400
		s.Interval = 60
		s.SetId()
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range getMetricData(2, 2, 5, 10, "metric.foo") {
		s.Time = 1 * 86400
		ix.AddOrUpdate(s, 1)
	}

	Convey("When listing root nodes", t, func() {
		Convey("root nodes for orgId 1", func() {
			nodes, err := ix.Find(1, "*", 0)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 2)
			So(nodes[0].Path, ShouldBeIn, "metric", "foo")
			So(nodes[1].Path, ShouldBeIn, "metric", "foo")
			So(nodes[0].Leaf, ShouldBeFalse)
		})
		Convey("root nodes for orgId 2", func() {
			nodes, err := ix.Find(2, "*", 0)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 1)
			So(nodes[0].Path, ShouldEqual, "metric")
			So(nodes[0].Leaf, ShouldBeFalse)
		})
	})

	Convey("When searching with GLOB", t, func() {
		nodes, err := ix.Find(2, "metric.{f*,demo}.*", 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 10)
		for _, n := range nodes {
			So(n.Leaf, ShouldBeFalse)
		}
	})

	Convey("When searching with multiple wildcards", t, func() {
		nodes, err := ix.Find(1, "*.*", 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 2)
		for _, n := range nodes {
			So(n.Leaf, ShouldBeFalse)
		}
	})

	Convey("When searching nodes not in public series", t, func() {
		nodes, err := ix.Find(1, "foo.demo.*", 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)
		Convey("When searching for specific series", func() {
			found, err := ix.Find(1, nodes[0].Path, 0)
			So(err, ShouldBeNil)
			So(found, ShouldHaveLength, 1)
			So(found[0].Path, ShouldEqual, nodes[0].Path)
		})
		Convey("When searching nodes that are children of a leaf", func() {
			found, err := ix.Find(1, nodes[0].Path+".*", 0)
			So(err, ShouldBeNil)
			So(found, ShouldHaveLength, 0)
		})
	})

	Convey("When searching with multiple wildcards mixed leaf/branch", t, func() {
		nodes, err := ix.Find(1, "*.demo.*", 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 15)
		for _, n := range nodes {
			if strings.HasPrefix(n.Path, "foo.demo") {
				So(n.Leaf, ShouldBeTrue)
				So(n.Defs, ShouldHaveLength, 2)
			} else {
				So(n.Leaf, ShouldBeFalse)
			}
		}
	})
	Convey("When searching nodes for unknown orgId", t, func() {
		nodes, err := ix.Find(4, "foo.demo.*", 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
	})

	Convey("When searching nodes that dont exist", t, func() {
		nodes, err := ix.Find(1, "foo.demo.blah.*", 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
	})

	Convey("When searching with from timestamp", t, func() {
		nodes, err := ix.Find(1, "*.demo.*", 4*86400)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 10)
		for _, n := range nodes {
			So(n.Path, ShouldNotContainSubstring, "foo.demo")
		}
		Convey("When searching with from timestamp on series with multiple defs.", func() {
			nodes, err := ix.Find(1, "*.demo.*", 2*86400)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 15)
			for _, n := range nodes {
				if strings.HasPrefix(n.Path, "foo.demo") {
					So(n.Defs, ShouldHaveLength, 1)
				}
			}
		})
	})

}

func TestDelete(t *testing.T) {
	ix := New()
	ix.Init()

	publicSeries := getMetricData(-1, 2, 5, 10, "metric.public")
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1")

	for _, s := range publicSeries {
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range org1Series {
		ix.AddOrUpdate(s, 1)
	}
	Convey("when deleting exact path", t, func() {
		defs, err := ix.Delete(1, org1Series[0].Name)
		So(err, ShouldBeNil)
		So(defs, ShouldHaveLength, 1)
		So(defs[0].Id, ShouldEqual, org1Series[0].Id)
		Convey("series should not be present in the metricDef index", func() {
			_, ok := ix.Get(org1Series[0].Id)
			So(ok, ShouldEqual, false)
			Convey("series should not be present in searchs", func() {
				nodes := strings.Split(org1Series[0].Name, ".")
				branch := strings.Join(nodes[0:len(nodes)-2], ".")
				found, err := ix.Find(1, branch+".*.*", 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 4)
				for _, n := range found {
					So(n.Path, ShouldNotEqual, org1Series[0].Name)
				}
			})
		})
	})

	Convey("when deleting by wildcard", t, func() {
		defs, err := ix.Delete(1, "metric.org1.*")
		So(err, ShouldBeNil)
		t.Log(len(defs))
		So(defs, ShouldHaveLength, 4)
		Convey("series should not be present in the metricDef index", func() {
			for _, def := range org1Series {
				_, ok := ix.Get(def.Id)
				So(ok, ShouldEqual, false)
			}
			Convey("series should not be present in searchs", func() {
				for _, def := range org1Series {
					nodes := strings.Split(def.Name, ".")
					branch := strings.Join(nodes[0:len(nodes)-1], ".")
					found, err := ix.Find(1, branch+".*", 0)
					So(err, ShouldBeNil)
					So(found, ShouldHaveLength, 0)
				}
				found, err := ix.Find(1, "metric.*", 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 1)
				So(found[0].Path, ShouldEqual, "metric.public")
			})
		})
	})
}

func TestMixedBranchLeaf(t *testing.T) {
	ix := New()
	ix.Init()

	first := &schema.MetricData{
		Name:     "foo.bar",
		Metric:   "foo.bar",
		OrgId:    1,
		Interval: 10,
	}
	second := &schema.MetricData{
		Name:     "foo.bar.baz",
		Metric:   "foo.bar.baz",
		OrgId:    1,
		Interval: 10,
	}
	third := &schema.MetricData{
		Name:     "foo",
		Metric:   "foo",
		OrgId:    1,
		Interval: 10,
	}
	first.SetId()
	second.SetId()
	third.SetId()

	Convey("when adding the first metric", t, func() {

		err := ix.AddOrUpdate(first, 1)
		So(err, ShouldBeNil)
		Convey("we should be able to add a leaf under another leaf", func() {

			err = ix.AddOrUpdate(second, 1)
			So(err, ShouldBeNil)
			_, ok := ix.Get(second.Id)
			So(ok, ShouldEqual, true)
			defs := ix.List(1)
			So(len(defs), ShouldEqual, 2)
		})
		Convey("we should be able to add a leaf that collides with an existing branch", func() {

			err = ix.AddOrUpdate(third, 1)
			So(err, ShouldBeNil)
			_, ok := ix.Get(third.Id)
			So(ok, ShouldEqual, true)
			defs := ix.List(1)
			So(len(defs), ShouldEqual, 3)
		})
	})
}

func TestPrune(t *testing.T) {
	ix := New()
	ix.Init()

	// add old series
	for _, s := range getSeriesNames(2, 5, "metric.bah") {
		d := &schema.MetricData{
			Name:     s,
			Metric:   s,
			OrgId:    1,
			Interval: 10,
			Time:     1,
		}
		d.SetId()
		ix.AddOrUpdate(d, 1)
	}
	//new series
	for _, s := range getSeriesNames(2, 5, "metric.foo") {
		d := &schema.MetricData{
			Name:     s,
			Metric:   s,
			OrgId:    1,
			Interval: 10,
			Time:     10,
		}
		d.SetId()
		ix.AddOrUpdate(d, 1)
	}
	Convey("after populating index", t, func() {
		defs := ix.List(-1)
		So(defs, ShouldHaveLength, 10)
	})
	Convey("When purging old series", t, func() {
		purged, err := ix.Prune(1, time.Unix(2, 0))
		So(err, ShouldBeNil)
		So(purged, ShouldHaveLength, 5)
		nodes, err := ix.Find(1, "metric.bah.*", 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
		nodes, err = ix.Find(1, "metric.foo.*", 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)

	})
	Convey("after purge", t, func() {
		defs := ix.List(-1)
		So(defs, ShouldHaveLength, 5)
		newDef := defs[0]
		newDef.Interval = 30
		newDef.LastUpdate = 100
		newDef.SetId()
		ix.AddOrUpdateDef(&newDef)
		Convey("When purging old series", func() {
			purged, err := ix.Prune(1, time.Unix(12, 0))
			So(err, ShouldBeNil)
			So(purged, ShouldHaveLength, 4)
			nodes, err := ix.Find(1, "metric.foo.*", 0)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 1)
		})
	})

}

func BenchmarkIndexing(b *testing.B) {
	ix := New()
	ix.Init()

	var series string
	var data *schema.MetricData
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		series = "some.metric." + strconv.Itoa(n)
		data = &schema.MetricData{
			Name:     series,
			Metric:   series,
			Interval: 10,
			OrgId:    1,
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
}
