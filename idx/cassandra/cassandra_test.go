package cassandra

import (
	"crypto/rand"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
	. "github.com/smartystreets/goconvey/convey"
)

type testIterator struct {
	rows []cassRow
}

type cassRow struct {
	id         string
	orgId      int
	partition  int32
	name       string
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

	if len(dest) < 9 {
		return false
	}

	row := i.rows[0]
	*(dest[0].(*string)) = row.id
	*(dest[1].(*int)) = row.orgId
	*(dest[2].(*int32)) = row.partition
	*(dest[3].(*string)) = row.name
	*(dest[4].(*int)) = row.interval
	*(dest[5].(*string)) = row.unit
	*(dest[6].(*string)) = row.mtype
	*(dest[7].(*[]string)) = row.tags
	*(dest[8].(*int64)) = row.lastUpdate

	i.rows = i.rows[1:]

	return true
}

func (i *testIterator) Close() error {
	return nil
}

func init() {
	CliConfig.keyspace = "metrictank"
	CliConfig.hosts = ""
	CliConfig.consistency = "one"
	CliConfig.timeout = time.Second
	CliConfig.numConns = 1
	CliConfig.writeQueueSize = 1000
	CliConfig.protoVer = 4
	CliConfig.updateCassIdx = false

	cluster.Init("default", "test", time.Now(), "http", 6060)
}

func initForTests(c *CasIdx) error {
	return c.MemoryIndex.Init()
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

func getMetricData(orgId uint32, depth, count, interval int, prefix string) []*schema.MetricData {
	data := make([]*schema.MetricData, count)
	series := getSeriesNames(depth, count, prefix)
	for i, s := range series {

		data[i] = &schema.MetricData{
			Name:     s,
			OrgId:    int(orgId),
			Interval: interval,
		}
		data[i].SetId()
	}
	return data
}

func TestGetAddKey(t *testing.T) {
	idx.OrgIdPublic = 100
	defer func() { idx.OrgIdPublic = 0 }()

	ix := New(CliConfig)
	initForTests(ix)

	publicSeries := getMetricData(idx.OrgIdPublic, 2, 5, 10, "metric.public")
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1")
	org2Series := getMetricData(2, 2, 5, 10, "metric.org2")

	for _, series := range [][]*schema.MetricData{publicSeries, org1Series, org2Series} {
		orgId := uint32(series[0].OrgId)
		Convey(fmt.Sprintf("When indexing metrics for orgId %d", orgId), t, func() {
			for _, s := range series {
				mkey, err := schema.MKeyFromString(s.Id)
				if err != nil {
					t.Fatalf("could not get key for %q: %s", s.Id, err)
				}
				ix.AddOrUpdate(mkey, s, 1)
			}
			Convey(fmt.Sprintf("Then listing metrics for OrgId %d", orgId), func() {
				defs := ix.List(orgId)
				numSeries := len(series)
				if orgId != idx.OrgIdPublic {
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
			mkey, err := schema.MKeyFromString(series.Id)
			if err != nil {
				t.Fatal(err)
			}

			ix.AddOrUpdate(mkey, series, 1)
		}
		Convey("then listing metrics", func() {
			defs := ix.List(1)
			So(defs, ShouldHaveLength, 15)
		})
	})
}

func TestAddToWriteQueue(t *testing.T) {
	originalUpdateCassIdx := CliConfig.updateCassIdx
	originalUpdateInterval := CliConfig.updateInterval
	originalWriteQSize := CliConfig.writeQueueSize

	defer func() {
		CliConfig.updateCassIdx = originalUpdateCassIdx
		CliConfig.updateInterval = originalUpdateInterval
		CliConfig.writeQueueSize = originalWriteQSize
	}()

	CliConfig.updateCassIdx = true
	CliConfig.updateInterval = 10
	CliConfig.writeQueueSize = 5
	ix := New(CliConfig)
	initForTests(ix)
	metrics := getMetricData(1, 2, 5, 10, "metric.demo")
	Convey("When writeQueue is enabled", t, func() {
		Convey("When new metrics being added", func() {
			for _, s := range metrics {
				mkey, err := schema.MKeyFromString(s.Id)
				if err != nil {
					t.Fatal(err)
				}

				ix.AddOrUpdate(mkey, s, 1)
				select {
				case wr := <-ix.writeQueue:
					if wr.def.Id != mkey {
						t.Fatalf("wrong key")
					}
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
				mkey, err := schema.MKeyFromString(s.Id)
				if err != nil {
					t.Fatal(err)
				}

				s.Time = time.Now().Unix()
				ix.AddOrUpdate(mkey, s, 1)
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
				mkey, err := schema.MKeyFromString(s.Id)
				if err != nil {
					t.Fatal(err)
				}

				archive, _ := ix.Get(mkey)
				archive.LastSave = uint32(time.Now().Unix() - 100)
				ix.UpdateArchive(archive)
			}
			for _, s := range metrics {
				mkey, err := schema.MKeyFromString(s.Id)
				if err != nil {
					t.Fatal(err)
				}
				ix.AddOrUpdate(mkey, s, 1)
				select {
				case wr := <-ix.writeQueue:
					if wr.def.Id != mkey {
						t.Fatal("wrong key")
					}
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
				mkey, err := schema.MKeyFromString(s.Id)
				if err != nil {
					t.Fatal(err)
				}
				ix.AddOrUpdate(mkey, s, 1)
			}
			//it should take at least 1 second to add the defs, as the queue will be full
			// until the above goroutine empties it, leading to a blocking write.
			So(time.Now(), ShouldHappenAfter, pre.Add(time.Second))
		})
	})
	ix.MemoryIndex.Stop()
	close(ix.writeQueue)
}

func TestFind(t *testing.T) {
	idx.OrgIdPublic = 100
	defer func() { idx.OrgIdPublic = 0 }()
	ix := New(CliConfig)
	initForTests(ix)
	for _, s := range getMetricData(idx.OrgIdPublic, 2, 5, 10, "metric.demo") {
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, 1)
	}
	for _, s := range getMetricData(1, 2, 5, 10, "metric.demo") {
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, 1)
	}
	for _, s := range getMetricData(1, 1, 5, 10, "foo.demo") {
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, 1)
		s.Interval = 60
		s.SetId()
		mkey, err = schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, 1)
	}
	for _, s := range getMetricData(2, 2, 5, 10, "metric.foo") {
		mkey, err := schema.MKeyFromString(s.Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, s, 1)
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

	Convey("When searching nodes that don't exist", t, func() {
		nodes, err := ix.Find(1, "foo.demo.blah.*", 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
	})

}

func BenchmarkIndexing(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	cluster.Manager.SetPartitions([]int32{1})
	CliConfig.keyspace = "metrictank"
	CliConfig.hosts = "localhost:9042"
	CliConfig.consistency = "one"
	CliConfig.timeout = time.Second
	CliConfig.numConns = 10
	CliConfig.writeQueueSize = 10
	CliConfig.protoVer = 4
	CliConfig.updateInterval = time.Hour
	CliConfig.updateCassIdx = true
	ix := New(CliConfig)
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
			Interval: 10,
			OrgId:    1,
		}
		data.SetId()
		mkey, err := schema.MKeyFromString(data.Id)
		if err != nil {
			panic(err)
		}
		ix.AddOrUpdate(mkey, data, 1)
	}
}

func BenchmarkLoad(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	cluster.Manager.SetPartitions([]int32{1})
	CliConfig.keyspace = "metrictank"
	CliConfig.hosts = "localhost:9042"
	CliConfig.consistency = "one"
	CliConfig.timeout = time.Second
	CliConfig.numConns = 10
	CliConfig.writeQueueSize = 10
	CliConfig.protoVer = 4
	CliConfig.updateInterval = time.Hour
	CliConfig.updateCassIdx = true
	ix := New(CliConfig)

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
	ix = New(CliConfig)
	ix.Init()
	ix.Stop()
}

func BenchmarkIndexingWithUpdates(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	cluster.Manager.SetPartitions([]int32{1})
	CliConfig.keyspace = "metrictank"
	CliConfig.hosts = "localhost:9042"
	CliConfig.consistency = "one"
	CliConfig.timeout = time.Second
	CliConfig.numConns = 10
	CliConfig.writeQueueSize = 10
	CliConfig.protoVer = 4
	CliConfig.updateInterval = time.Hour
	CliConfig.updateCassIdx = true
	ix := New(CliConfig)
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
		mkey, err := schema.MKeyFromString(updates[n].Id)
		if err != nil {
			b.Fatal(err)
		}
		ix.AddOrUpdate(mkey, updates[n], 1)
	}
	ix.Stop()
}

func TestPruneStaleOnLoad(t *testing.T) {

	now := time.Now()

	iter := testIterator{}
	memory.IndexRules = conf.IndexRules{
		Rules: []conf.IndexRule{
			{
				Name:     "longterm",
				Pattern:  regexp.MustCompile("^long"),
				MaxStale: time.Duration(365*24) * time.Hour,
			},
			{
				Name:     "default",
				Pattern:  regexp.MustCompile("foobar"),
				MaxStale: time.Duration(24*7) * time.Hour,
			},
		},
		Default: conf.IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: 0,
		},
	}
	iter.rows = append(iter.rows, cassRow{
		id:         test.GetMKey(1).String(),
		name:       "longtermrecentenough",
		interval:   1,
		lastUpdate: now.Add(-350 * 24 * time.Hour).Unix(),
	})
	iter.rows = append(iter.rows, cassRow{
		id:         test.GetMKey(2).String(),
		name:       "longtermtooold",
		interval:   1,
		lastUpdate: now.Add(-380 * 24 * time.Hour).Unix(),
	})
	iter.rows = append(iter.rows, cassRow{
		id:         test.GetMKey(3).String(),
		name:       "foobarrecentenough",
		interval:   3,
		lastUpdate: now.Add(-5 * 24 * time.Hour).Unix(),
	})
	iter.rows = append(iter.rows, cassRow{
		id:         test.GetMKey(4).String(),
		name:       "foobartooold",
		interval:   3,
		lastUpdate: now.Add(-9 * 24 * time.Hour).Unix(),
	})
	iter.rows = append(iter.rows, cassRow{
		id:         test.GetMKey(5).String(),
		name:       "default-super-old-but-never-pruned",
		interval:   1,
		lastUpdate: now.Add(-2 * 365 * 24 * time.Hour).Unix(),
	})

	idx := &CasIdx{}
	defs := idx.load(nil, &iter, now)

	exp := []schema.MKey{
		test.GetMKey(1),
		test.GetMKey(3),
		test.GetMKey(5),
	}
	var got []schema.MKey
	for _, def := range defs {
		got = append(got, def.Id)
	}
	sort.Sort(MKeyAsc(got))
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("Expected defs %v, got %v", exp, got)
	}
}

func TestPruneStaleOnLoadWithTags(t *testing.T) {

	now := time.Now()

	iter := testIterator{}
	memory.IndexRules = conf.IndexRules{
		Rules: []conf.IndexRule{
			{
				Name:     "longterm",
				Pattern:  regexp.MustCompile("long=term"),
				MaxStale: time.Duration(365*24) * time.Hour,
			},
			{
				Name:     "foo=bar",
				Pattern:  regexp.MustCompile("foo=bar"),
				MaxStale: time.Duration(24*7) * time.Hour,
			},
		},
		Default: conf.IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: 0,
		},
	}
	iter.rows = append(iter.rows, cassRow{
		id:         test.GetMKey(1).String(),
		orgId:      1,
		partition:  1,
		name:       "met1",
		interval:   1,
		lastUpdate: now.Add(-380 * 24 * time.Hour).Unix(),
		tags:       []string{"tag1=val1"},
	})
	// because this one has been recently updated, the one before that is not pruned either
	iter.rows = append(iter.rows, cassRow{
		id:         test.GetMKey(2).String(),
		orgId:      1,
		partition:  1,
		name:       "met1",
		interval:   2,
		lastUpdate: now.Add(-350 * 24 * time.Hour).Unix(),
		tags:       []string{"tag1=val1"},
	})
	// next 2 are to show that tag matching works
	iter.rows = append(iter.rows, cassRow{
		id:         test.GetMKey(3).String(),
		orgId:      1,
		partition:  1,
		name:       "met1",
		interval:   3,
		lastUpdate: now.Add(-8 * 24 * time.Hour).Unix(), // this one will expire
		tags:       []string{"tag1=val1;foo=bar"},
	})
	iter.rows = append(iter.rows, cassRow{
		id:         test.GetMKey(4).String(),
		orgId:      1,
		partition:  1,
		name:       "met1",
		interval:   4,
		lastUpdate: now.Add(-8 * 24 * time.Hour).Unix(), // this one won't because it doesn't match the tag
		tags:       []string{"tag1=val1;foo=baz"},
	})

	idx := &CasIdx{}
	defs := idx.load(nil, &iter, now)
	exp := []schema.MKey{
		test.GetMKey(1),
		test.GetMKey(2),
		test.GetMKey(4),
	}
	var got []schema.MKey
	for _, def := range defs {
		got = append(got, def.Id)
	}
	sort.Sort(MKeyAsc(got))
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("Expected defs %v, got %v", exp, got)
	}
}

type MKeyAsc []schema.MKey

func (m MKeyAsc) Len() int      { return len(m) }
func (m MKeyAsc) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m MKeyAsc) Less(i, j int) bool {
	for pos, vi := range m[i].Key {
		vj := m[j].Key[pos]
		if vi < vj {
			return true
		}
	}
	return false
}
