package cassandra

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/idx"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/raintank/schema.v1"
)

type testIterator struct {
	rows []cassRow
}

type cassRow struct {
	id         string
	orgId      int
	partition  int32
	name       string
	metric     string
	interval   int
	unit       string
	mtype      string
	tags       []string
	lastUpdate int64
}

func (i *testIterator) Scan(dest ...interface{}) bool {
	if len(i.rows) == 0 {
		return false
	}

	if len(dest) < 10 {
		return false
	}

	row := i.rows[0]
	*(dest[0].(*string)) = row.id
	*(dest[1].(*int)) = row.orgId
	*(dest[2].(*int32)) = row.partition
	*(dest[3].(*string)) = row.name
	*(dest[4].(*string)) = row.metric
	*(dest[5].(*int)) = row.interval
	*(dest[6].(*string)) = row.unit
	*(dest[7].(*string)) = row.mtype
	*(dest[8].(*[]string)) = row.tags
	*(dest[9].(*int64)) = row.lastUpdate

	i.rows = i.rows[1:]

	return true
}

func (i *testIterator) Close() error {
	return nil
}

func init() {
	keyspace = "metrictank"
	hosts = ""
	consistency = "one"
	timeout = time.Second
	numConns = 1
	writeQueueSize = 1000
	protoVer = 4
	updateCassIdx = false

	cluster.Init("default", "test", time.Now(), "http", 6060)
}

func initForTests(c *CasIdx) error {
	return c.MemoryIdx.Init()
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
	initForTests(ix)

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

func TestAddToWriteQueue(t *testing.T) {
	originalUpdateCassIdx := updateCassIdx
	originalUpdateInterval := updateInterval
	originalWriteQSize := writeQueueSize

	defer func() {
		updateCassIdx = originalUpdateCassIdx
		updateInterval = originalUpdateInterval
		writeQueueSize = originalWriteQSize
	}()

	updateCassIdx = true
	updateInterval = 10
	writeQueueSize = 5
	ix := New()
	initForTests(ix)
	metrics := getMetricData(1, 2, 5, 10, "metric.demo")
	Convey("When writeQueue is enabled", t, func() {
		Convey("When new metrics being added", func() {
			for _, s := range metrics {
				ix.AddOrUpdate(s, 1)
				select {
				case wr := <-ix.writeQueue:
					So(wr.def.Id, ShouldEqual, s.Id)
					archive, inMem := ix.Get(wr.def.Id)
					So(inMem, ShouldBeTrue)
					now := uint32(time.Now().Unix())
					So(archive.LastSave, ShouldBeBetweenOrEqual, now-1, now+1)
				case <-time.After(time.Second):
					t.Fail()
				}
			}
		})
		Convey("When existing metrics are added and lastSave is recent", func() {
			for _, s := range metrics {
				s.Time = time.Now().Unix()
				ix.AddOrUpdate(s, 1)
			}
			wrCount := 0

		LOOP_WR:
			for {
				select {
				case <-ix.writeQueue:
					wrCount++
				default:
					break LOOP_WR
				}
			}
			So(wrCount, ShouldEqual, 0)
		})
		Convey("When existing metrics are added and lastSave is old", func() {
			for _, s := range metrics {
				s.Time = time.Now().Unix()
				archive, _ := ix.Get(s.Id)
				archive.LastSave = uint32(time.Now().Unix() - 100)
				ix.Update(archive)
			}
			for _, s := range metrics {
				ix.AddOrUpdate(s, 1)
				select {
				case wr := <-ix.writeQueue:
					So(wr.def.Id, ShouldEqual, s.Id)
					archive, inMem := ix.Get(wr.def.Id)
					So(inMem, ShouldBeTrue)
					now := uint32(time.Now().Unix())
					So(archive.LastSave, ShouldBeBetweenOrEqual, now-1, now+1)
				case <-time.After(time.Second):
					t.Fail()
				}
			}
		})
		Convey("When new metrics are added and writeQueue is full", func() {
			newMetrics := getMetricData(1, 2, 6, 10, "metric.demo2")
			pre := time.Now()
			go func() {
				time.Sleep(time.Second)
				//drain the writeQueue
				for range ix.writeQueue {
					continue
				}
			}()

			for _, s := range newMetrics {
				ix.AddOrUpdate(s, 1)
			}
			//it should take at least 1 second to add the defs, as the queue will be full
			// until the above goroutine empties it, leading to a blocking write.
			So(time.Now(), ShouldHappenAfter, pre.Add(time.Second))
		})
	})
	ix.MemoryIdx.Stop()
	close(ix.writeQueue)
}

func TestFind(t *testing.T) {
	ix := New()
	initForTests(ix)
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
	updateCassIdx = true
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
	updateCassIdx = true
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

func BenchmarkIndexingWithUpdates(b *testing.B) {
	cluster.Manager.SetPartitions([]int32{1})
	keyspace = "metrictank"
	hosts = "localhost:9042"
	consistency = "one"
	timeout = time.Second
	numConns = 10
	writeQueueSize = 10
	protoVer = 4
	updateInterval = time.Hour
	updateCassIdx = true
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
	insertDefs(ix, b.N)
	updates := make([]*schema.MetricData, b.N)
	var series string
	var data *schema.MetricData
	for n := 0; n < b.N; n++ {
		series = "some.metric." + strconv.Itoa(n)
		data = &schema.MetricData{
			Name:     series,
			Metric:   series,
			Interval: 10,
			OrgId:    1,
			Time:     10,
		}
		data.SetId()
		updates[n] = data
	}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ix.AddOrUpdate(updates[n], 1)
	}
	ix.Stop()
}

func TestPruneStaleOnLoad(t *testing.T) {
	iter := testIterator{}
	iter.rows = append(iter.rows, cassRow{
		id:         "1",
		orgId:      1,
		partition:  1,
		name:       "met1",
		metric:     "met1",
		interval:   1,
		lastUpdate: 1,
	})
	iter.rows = append(iter.rows, cassRow{
		id:         "2",
		orgId:      1,
		partition:  1,
		name:       "met1",
		metric:     "met1",
		interval:   3,
		lastUpdate: 2,
	})
	iter.rows = append(iter.rows, cassRow{
		id:         "3",
		orgId:      1,
		partition:  1,
		name:       "met2",
		metric:     "met2",
		interval:   1,
		lastUpdate: 1,
	})
	iter.rows = append(iter.rows, cassRow{
		id:         "4",
		orgId:      1,
		partition:  1,
		name:       "met2",
		metric:     "met2",
		interval:   3,
		lastUpdate: 1,
	})

	idx := &CasIdx{}
	var defs []schema.MetricDefinition
	defs = idx.load(defs, &iter, uint32(2))

	expectedLen := 2
	if len(defs) != expectedLen {
		t.Fatalf("Expected %d defs, but got %d", expectedLen, len(defs))
	}

	if defs[0].Id != "1" || defs[1].Id != "2" {
		t.Fatalf("Expected IDs 1 & 2, but got %s & %s", defs[0].Id, defs[1].Id)
	}
}

func TestPruneStaleOnLoadWithTags(t *testing.T) {
	iter := testIterator{}
	iter.rows = append(iter.rows, cassRow{
		id:         "1",
		orgId:      1,
		partition:  1,
		name:       "met1",
		metric:     "met1",
		interval:   1,
		lastUpdate: 1,
		tags:       []string{"tag1=val1"},
	})
	iter.rows = append(iter.rows, cassRow{
		id:         "2",
		orgId:      1,
		partition:  1,
		name:       "met1",
		metric:     "met1",
		interval:   2,
		lastUpdate: 2,
		tags:       []string{"tag1=val1"},
	})
	iter.rows = append(iter.rows, cassRow{
		id:         "3",
		orgId:      1,
		partition:  1,
		name:       "met1",
		metric:     "met1",
		interval:   3,
		lastUpdate: 1,
		tags:       []string{"tag1=val1;tag2=val2"},
	})
	iter.rows = append(iter.rows, cassRow{
		id:         "4",
		orgId:      1,
		partition:  1,
		name:       "met1",
		metric:     "met1",
		interval:   4,
		lastUpdate: 1,
		tags:       []string{"tag1=val1;tag2=val2"},
	})

	idx := &CasIdx{}
	var defs []schema.MetricDefinition
	defs = idx.load(defs, &iter, uint32(2))

	expectedLen := 2
	if len(defs) != expectedLen {
		t.Fatalf("Expected %d defs, but got %d", expectedLen, len(defs))
	}

	if defs[0].Id != "1" || defs[1].Id != "2" {
		t.Fatalf("Expected IDs 1 & 2, but got %s & %s", defs[0].Id, defs[1].Id)
	}
}
