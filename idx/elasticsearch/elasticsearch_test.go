package elasticsearch

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/raintank/met/helper"
	"github.com/raintank/metrictank/idx"
	//"github.com/raintank/worldping-api/pkg/log"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/raintank/schema.v1"
)

func TestES(t *testing.T) {

	// use or mock HTTP transport so we dont need to talk to a real ES server
	rt := newMockTransport()
	http.DefaultClient.Transport = rt

	// Initialize our config flags
	esHosts = "localhost:9200"
	esMaxConns = 10
	esMaxBufferDocs = 2
	esRetryInterval = time.Second
	esBufferDelayMax = time.Millisecond * 100
	esIndex = "metrics"

	rt.Set("PUT http://localhost:9200/metrics", handleCreateIndexOk)
	rt.Set("POST http://localhost:9200/metrics/metric_index/_search?scroll=1m&size=1000", handleSearchEmpty)
	rt.Set("POST http://localhost:9200/_search/scroll?scroll=1m", handleSearchEmpty)
	rt.Set("POST http://localhost:9200/_bulk?refresh=true", handleBulkOk)

	Convey("When initializing empty ES index", t, func() {
		// create our EsIndex object
		ix := New()
		// add a request tracer to notify over a channle the details of every request made
		reqChan := make(chan requestTrace, 100)
		ix.Conn.RequestTracer = func(method, urlStr, body string) {
			reqChan <- requestTrace{method: method, url: urlStr, body: body}
		}

		stats, _ := helper.New(false, "", "standard", "metrictank", "")
		err := ix.Init(stats)
		So(err, ShouldBeNil)

		// we should see a PUT request to create the index mapping
		req := <-reqChan
		So(req.method, ShouldEqual, "PUT")
		So(req.url, ShouldEqual, fmt.Sprintf("http://localhost:9200/%s", esIndex))

		// we should see a POST request to search the index for rebuilding the Index
		req = <-reqChan
		So(req.method, ShouldEqual, "POST")
		So(req.url, ShouldEqual, fmt.Sprintf("http://localhost:9200/%s/metric_index/_search?scroll=1m&size=1000", esIndex))
		ix.Stop()
	})

	Convey("When initilizing existing index", t, func() {
		rt.Set("HEAD http://localhost:9200/metrics", handleOk)
		rt.Set("POST http://localhost:9200/metrics/metric_index/_search?scroll=1m&size=1000", handleSearch1Item)

		// create our EsIndex object
		ix := New()
		// add a request tracer to notify over a channle the details of every request made
		reqChan := make(chan requestTrace, 100)
		ix.Conn.RequestTracer = func(method, urlStr, body string) {
			reqChan <- requestTrace{method: method, url: urlStr, body: body}
		}
		stats, _ := helper.New(false, "", "standard", "metrictank", "")
		err := ix.Init(stats)
		So(err, ShouldBeNil)

		defs := ix.List(1)
		So(defs, ShouldHaveLength, 1)

		// we should see a POST request to search the index for rebuilding the Index
		req := <-reqChan
		So(req.method, ShouldEqual, "POST")
		So(req.url, ShouldEqual, fmt.Sprintf("http://localhost:9200/%s/metric_index/_search?scroll=1m&size=1000", esIndex))
		req = <-reqChan
		So(req.method, ShouldEqual, "POST")
		So(req.url, ShouldEqual, "http://localhost:9200/_search/scroll?scroll=1m")

		Convey("When adding def to index", func() {
			data := &schema.MetricData{Name: "test0.data", Metric: "test0.data", OrgId: 1}
			data.SetId()
			ix.Add(data)
			req = <-reqChan
			So(req.method, ShouldEqual, "POST")
			So(req.url, ShouldEqual, "http://localhost:9200/_bulk?refresh=true")
			So(req.body, ShouldContainSubstring, data.Id)
			//retry queue should be empty
			So(ix.retryBuf.Items(), ShouldHaveLength, 0)
		})
		Convey("When adding to index fails", func() {
			rt.Set("POST http://localhost:9200/_bulk?refresh=true", handleBulkHalfError)
			data := &schema.MetricData{Name: "test0.data", Metric: "test0.data", OrgId: 1}
			data.SetId()
			ix.Add(data)
			data.Name = "test2.data"
			data.Metric = "Test2.data"
			data.SetId()
			ix.Add(data)
			req = <-reqChan
			So(req.method, ShouldEqual, "POST")
			So(req.url, ShouldEqual, "http://localhost:9200/_bulk?refresh=true")
			So(req.body, ShouldContainSubstring, "test2.data")
			So(req.body, ShouldContainSubstring, "test0.data")
			//retry should have 1 item
			time.Sleep(time.Millisecond * 10)
			So(ix.retryBuf.Items(), ShouldHaveLength, 1)
			So(ix.retryBuf.Items()[0].Id, ShouldEqual, data.Id)
			req = <-reqChan
			time.Sleep(time.Millisecond * 50)
			So(req.method, ShouldEqual, "POST")
			So(req.url, ShouldEqual, "http://localhost:9200/_bulk?refresh=true")
			So(req.body, ShouldContainSubstring, "test2.data")
			So(ix.retryBuf.Items(), ShouldHaveLength, 0)
		})
		ix.Stop()
	})
}

func TestGetAddKey(t *testing.T) {
	// use or mock HTTP transport so we dont need to talk to a real ES server
	rt := newMockTransport()
	http.DefaultClient.Transport = rt

	// Initialize our config flags
	esHosts = "localhost:9200"
	esMaxConns = 10
	esMaxBufferDocs = 2
	esRetryInterval = time.Second
	esBufferDelayMax = time.Millisecond * 100
	esIndex = "metrics"

	rt.Response[fmt.Sprintf("PUT http://localhost:9200/%s", esIndex)] = handleCreateIndexOk
	rt.Response[fmt.Sprintf("POST http://localhost:9200/%s/metric_index/_search?scroll=1m&size=1000", esIndex)] = handleSearchEmpty
	rt.Response["POST http://localhost:9200/_bulk?refresh=true"] = handleBulkOk

	ix := New()
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	ix.Init(stats)
	defer ix.Stop()

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
	// use or mock HTTP transport so we dont need to talk to a real ES server
	rt := newMockTransport()
	http.DefaultClient.Transport = rt

	// Initialize our config flags
	esHosts = "localhost:9200"
	esMaxConns = 10
	esMaxBufferDocs = 2
	esRetryInterval = time.Second
	esBufferDelayMax = time.Millisecond * 100
	esIndex = "metrics"

	rt.Response[fmt.Sprintf("PUT http://localhost:9200/%s", esIndex)] = handleCreateIndexOk
	rt.Response[fmt.Sprintf("POST http://localhost:9200/%s/metric_index/_search?scroll=1m&size=1000", esIndex)] = handleSearchEmpty
	rt.Response["POST http://localhost:9200/_bulk?refresh=true"] = handleBulkOk

	ix := New()
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	ix.Init(stats)
	defer ix.Stop()

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

func TestDelete(t *testing.T) {
	// use or mock HTTP transport so we dont need to talk to a real ES server
	rt := newMockTransport()
	http.DefaultClient.Transport = rt

	// Initialize our config flags
	esHosts = "localhost:9200"
	esMaxConns = 10
	esMaxBufferDocs = 2
	esRetryInterval = time.Second
	esBufferDelayMax = time.Millisecond * 100
	esIndex = "metrics"

	rt.Response[fmt.Sprintf("PUT http://localhost:9200/%s", esIndex)] = handleCreateIndexOk
	rt.Response[fmt.Sprintf("POST http://localhost:9200/%s/metric_index/_search?scroll=1m&size=1000", esIndex)] = handleSearchEmpty
	rt.Response["POST http://localhost:9200/_bulk?refresh=true"] = handleBulkOk

	ix := New()
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	ix.Init(stats)
	defer ix.Stop()

	publicSeries := getMetricData(-1, 2, 5, 10, "metric.public")
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1")

	for _, s := range publicSeries {
		ix.Add(s)
	}
	for _, s := range org1Series {
		ix.Add(s)
	}
	Convey("when deleting exact path", t, func() {
		defs, err := ix.Delete(1, org1Series[0].Name)
		So(err, ShouldBeNil)
		So(defs, ShouldHaveLength, 1)
		Convey("series should not be present in the metricDef index", func() {
			_, err := ix.Get(org1Series[0].Id)
			So(err, ShouldEqual, idx.DefNotFound)
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
		So(defs, ShouldHaveLength, 4)
		Convey("series should not be present in the metricDef index", func() {
			for _, def := range org1Series {
				_, err := ix.Get(def.Id)
				So(err, ShouldEqual, idx.DefNotFound)
			}
			Convey("series should not be present in searches", func() {
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

func BenchmarkIndexing(b *testing.B) {
	esHosts = "localhost:9200"
	esMaxConns = 10
	esMaxBufferDocs = 1000
	esRetryInterval = time.Hour
	esBufferDelayMax = time.Second
	esIndex = "metrics"

	ix := New()
	ix.Conn.DeleteIndex(esIndex)
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	err := ix.Init(stats)
	if err != nil {
		b.Fatalf("can't connect to ES: %s", err)
	}

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
			OrgId:    (n % 15) + 1,
		}
		data.SetId()
		ix.Add(data)
	}
	ix.Stop()
}
