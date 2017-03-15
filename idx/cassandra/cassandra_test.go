package cassandra

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/raintank/schema.v1"
)

func init() {
	keyspace = "metrictank"
	hosts = ""
	consistency = "one"
	timeout = time.Second
	numConns = 1
	writeQueueSize = 1000
	protoVer = 4

	cluster.Init("default", "test", time.Now(), "http", 6060)
}

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
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range getMetricData(1, 2, 5, 10, "metric.demo") {
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range getMetricData(1, 1, 5, 10, "foo.demo") {
		ix.AddOrUpdate(s, 1)
		s.Interval = 60
		s.SetId()
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range getMetricData(2, 2, 5, 10, "metric.foo") {
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

}

func BenchmarkIndexing(b *testing.B) {
	cluster.Manager.SetPartitions([]int32{1})
	keyspace = "metrictank"
	hosts = "localhost:9042"
	consistency = "one"
	timeout = time.Second
	numConns = 10
	writeQueueSize = 10
	protoVer = 4
	updateInterval = time.Hour
	updateFuzzyness = 1.0
	ix := New()
	tmpSession, err := ix.cluster.CreateSession()
	if err != nil {
		b.Skipf("can't connect to cassandra: %s", err)
	}
	tmpSession.Query("TRUNCATE metrictank.metric_idx").Exec()
	tmpSession.Close()
	if err != nil {
		b.Skipf("can't connect to cassandra: %s", err)
	}
	ix.Init()

	b.ReportAllocs()
	b.ResetTimer()
	insertDefs(ix, b.N)
	ix.Stop()
}

func insertDefs(ix idx.MetricIndex, i int) {
	var series string
	var data *schema.MetricData
	for n := 0; n < i; n++ {
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

func BenchmarkLoad(b *testing.B) {
	cluster.Manager.SetPartitions([]int32{1})
	keyspace = "metrictank"
	hosts = "localhost:9042"
	consistency = "one"
	timeout = time.Second
	numConns = 10
	writeQueueSize = 10
	protoVer = 4
	updateInterval = time.Hour
	updateFuzzyness = 1.0
	ix := New()

	tmpSession, err := ix.cluster.CreateSession()
	if err != nil {
		b.Skipf("can't connect to cassandra: %s", err)
	}
	tmpSession.Query("TRUNCATE metrictank.metric_idx").Exec()
	tmpSession.Close()
	err = ix.Init()
	if err != nil {
		b.Skipf("can't initialize cassandra: %s", err)
	}

	insertDefs(ix, b.N)
	ix.Stop()

	b.ReportAllocs()
	b.ResetTimer()
	ix = New()
	ix.Init()
	ix.Stop()
}
