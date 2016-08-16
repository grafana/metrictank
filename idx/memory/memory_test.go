package memory

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/raintank/met/helper"
	"github.com/raintank/metrictank/idx"
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
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	ix.Init(stats)

	publicSeries := getMetricData(-1, 2, 5, 10, "metric.public")
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1")
	org2Series := getMetricData(2, 2, 5, 10, "metric.org2")

	for _, series := range [][]*schema.MetricData{publicSeries, org1Series, org2Series} {
		orgId := series[0].OrgId
		Convey(fmt.Sprintf("When indexing metrics for orgId %d", orgId), t, func() {
			for _, s := range series {
				ix.Add(s)
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
			ix.Add(series)
		}
		Convey("then listing metrics", func() {
			defs := ix.List(1)
			So(defs, ShouldHaveLength, 15)
		})
	})
}

func TestFind(t *testing.T) {
	ix := New()
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	ix.Init(stats)
	for _, s := range getMetricData(-1, 2, 5, 10, "metric.demo") {
		ix.Add(s)
	}
	for _, s := range getMetricData(1, 2, 5, 10, "metric.demo") {
		ix.Add(s)
	}
	for _, s := range getMetricData(1, 1, 5, 10, "foo.demo") {
		ix.Add(s)
		s.Interval = 60
		s.SetId()
		ix.Add(s)
	}
	for _, s := range getMetricData(2, 2, 5, 10, "metric.foo") {
		ix.Add(s)
	}

	Convey("When listing root nodes", t, func() {
		Convey("root nodes for orgId 1", func() {
			nodes := ix.Find(1, "*")
			So(nodes, ShouldHaveLength, 2)
			So(nodes[0].Path, ShouldBeIn, "metric", "foo")
			So(nodes[1].Path, ShouldBeIn, "metric", "foo")
			So(nodes[0].Leaf, ShouldBeFalse)
		})
		Convey("root nodes for orgId 2", func() {
			nodes := ix.Find(2, "*")
			So(nodes, ShouldHaveLength, 1)
			So(nodes[0].Path, ShouldEqual, "metric")
			So(nodes[0].Leaf, ShouldBeFalse)
		})
	})

	Convey("When searching with GLOB", t, func() {
		nodes := ix.Find(2, "metric.{f*,demo}.*")
		So(nodes, ShouldHaveLength, 10)
		for _, n := range nodes {
			So(n.Leaf, ShouldBeFalse)
		}
	})

	Convey("When searching with multiple wildcards", t, func() {
		nodes := ix.Find(1, "*.*")
		So(nodes, ShouldHaveLength, 2)
		for _, n := range nodes {
			So(n.Leaf, ShouldBeFalse)
		}
	})

	Convey("When searching nodes not in public series", t, func() {
		nodes := ix.Find(1, "foo.demo.*")
		So(nodes, ShouldHaveLength, 5)
		Convey("When searching for specific series", func() {
			found := ix.Find(1, nodes[0].Path)
			So(found, ShouldHaveLength, 1)
			So(found[0].Path, ShouldEqual, nodes[0].Path)
		})
		Convey("When searching nodes that are children of a leaf", func() {
			found := ix.Find(1, nodes[0].Path+".*")
			So(found, ShouldHaveLength, 0)
		})
	})

	Convey("When searching with multiple wildcards mixed leaf/branch", t, func() {
		nodes := ix.Find(1, "*.demo.*")
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
	Convey("When searching nodes for unkown orgId", t, func() {
		nodes := ix.Find(4, "foo.demo.*")
		So(nodes, ShouldHaveLength, 0)
	})

	Convey("When searching nodes that dont exist", t, func() {
		nodes := ix.Find(1, "foo.demo.blah.*")
		So(nodes, ShouldHaveLength, 0)
	})

}

func TestDelete(t *testing.T) {
	ix := New()
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	ix.Init(stats)

	publicSeries := getMetricData(-1, 2, 5, 10, "metric.public")
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1")

	for _, s := range publicSeries {
		ix.Add(s)
	}
	for _, s := range org1Series {
		ix.Add(s)
	}
	Convey("when deleting exact path", t, func() {
		ix.Delete(1, org1Series[0].Name)
		Convey("series should not be present in the metricDef index", func() {
			_, err := ix.Get(org1Series[0].Id)
			So(err, ShouldEqual, idx.DefNotFound)
		})
		Convey("series should not be present in searchs", func() {
			nodes := strings.Split(org1Series[0].Name, ".")
			branch := strings.Join(nodes[0:len(nodes)-2], ".")
			found := ix.Find(1, branch+".*.*")
			So(found, ShouldHaveLength, 4)
			for _, n := range found {
				So(n.Path, ShouldNotEqual, org1Series[0].Name)
			}
		})
	})

	Convey("when deleting by wildcard", t, func() {
		ix.Delete(1, "metric.org1.*")
		Convey("series should not be present in the metricDef index", func() {
			for _, def := range org1Series {
				_, err := ix.Get(def.Id)
				So(err, ShouldEqual, idx.DefNotFound)
			}
		})
		Convey("series should not be present in searchs", func() {
			for _, def := range org1Series {
				nodes := strings.Split(def.Name, ".")
				branch := strings.Join(nodes[0:len(nodes)-1], ".")
				found := ix.Find(1, branch+".*")
				So(found, ShouldHaveLength, 0)
			}
			found := ix.Find(1, "metric.*")
			So(found, ShouldHaveLength, 1)
			So(found[0].Path, ShouldEqual, "metric.public")
		})
	})
}

func BenchmarkIndexing(b *testing.B) {
	ix := New()
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	ix.Init(stats)

	var series string
	var data *schema.MetricData
	for n := 0; n < b.N; n++ {
		series = "some.metric." + strconv.Itoa(n)
		data = &schema.MetricData{
			Name:     series,
			Metric:   series,
			Interval: 10,
			OrgId:    1,
		}
		data.SetId()
		ix.Add(data)
	}
}
