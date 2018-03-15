package memory

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
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

func getMetricData(orgId, depth, count, interval int, prefix string, tagged bool) []*schema.MetricData {
	data := make([]*schema.MetricData, count)
	series := getSeriesNames(depth, count, prefix)

	for i, s := range series {
		data[i] = &schema.MetricData{
			Name:     s,
			Metric:   s,
			OrgId:    orgId,
			Interval: interval,
		}
		if tagged {
			data[i].Tags = []string{fmt.Sprintf("series_id=%d", i)}
		}
		data[i].SetId()
	}

	return data
}

// testWithAndWithoutTagSupport calls a test with the TagSupprt setting
// turned on and off. This is to verify that something works as expected
// no matter what this flag is set to, it does not mean that the behavior
// of a method should be changing dependent on that setting.
func testWithAndWithoutTagSupport(t *testing.T, f func(*testing.T)) {
	t.Helper()
	_tagSupport := TagSupport
	defer func() { TagSupport = _tagSupport }()

	TagSupport = true
	f(t)
	TagSupport = false
	f(t)
}

func TestGetAddKey(t *testing.T) {
	testWithAndWithoutTagSupport(t, testGetAddKey)
}

func testGetAddKey(t *testing.T) {
	ix := New()
	ix.Init()

	publicSeries := getMetricData(-1, 2, 5, 10, "metric.public", false)
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1", false)
	org2Series := getMetricData(2, 2, 5, 10, "metric.org2", false)

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
	testWithAndWithoutTagSupport(t, testFind)
}

func testFind(t *testing.T) {
	ix := New()
	ix.Init()
	for _, s := range getMetricData(-1, 2, 5, 10, "metric.demo", false) {
		s.Time = 10 * 86400
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range getMetricData(1, 2, 5, 10, "metric.demo", false) {
		s.Time = 10 * 86400
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range getMetricData(1, 1, 5, 10, "foo.demo", false) {
		s.Time = 1 * 86400
		ix.AddOrUpdate(s, 1)
		s.Time = 2 * 86400
		s.Interval = 60
		s.SetId()
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range getMetricData(2, 2, 5, 10, "metric.foo", false) {
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
	testWithAndWithoutTagSupport(t, testDelete)
}

func testDelete(t *testing.T) {
	ix := New()
	ix.Init()

	publicSeries := getMetricData(-1, 2, 5, 10, "metric.public", false)
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1", false)

	for _, s := range publicSeries {
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range org1Series {
		ix.AddOrUpdate(s, 1)
	}
}

func TestDeleteTagged(t *testing.T) {
	ix := New()
	ix.Init()

	publicSeries := getMetricData(-1, 2, 5, 10, "metric.public", true)
	org1Series := getMetricData(1, 2, 5, 10, "metric.org1", true)

	for _, s := range publicSeries {
		ix.AddOrUpdate(s, 1)
	}
	for _, s := range org1Series {
		ix.AddOrUpdate(s, 1)
	}

	Convey("when deleting by tag", t, func() {
		testName := schema.MetricDefinitionFromMetricData(org1Series[3]).NameWithTags()
		ids, err := ix.DeleteTagged(1, []string{testName})
		So(err, ShouldBeNil)
		So(ids, ShouldHaveLength, 1)
		So(ids[0].Id, ShouldEqual, org1Series[3].Id)
		Convey("series should not be present in the metricDef index", func() {
			nodes, err := ix.FindByTag(1, []string{"series_id=3"}, 0)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 0)
			Convey("but others should still be present", func() {
				nodes, err := ix.FindByTag(1, []string{"series_id=~[0-9]"}, 0)
				So(err, ShouldBeNil)
				So(nodes, ShouldHaveLength, 4)
			})
		})
	})
}

func TestDeleteNodeWith100kChildren(t *testing.T) {
	testWithAndWithoutTagSupport(t, testDeleteNodeWith100kChildren)
}

func testDeleteNodeWith100kChildren(t *testing.T) {
	ix := New()
	ix.Init()

	var data *schema.MetricData
	var key string
	for i := 1; i <= 100000; i++ {
		key = fmt.Sprintf("some.metric.%d.%d", i, i)
		data = &schema.MetricData{
			Name:     key,
			Metric:   key,
			OrgId:    1,
			Interval: 10,
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}

	Convey("when deleting 100k series", t, func() {
		type resp struct {
			defs []idx.Archive
			err  error
		}
		done := make(chan *resp)
		go func() {
			defs, err := ix.Delete(1, "some.*")
			done <- &resp{
				defs: defs,
				err:  err,
			}
		}()

		select {
		case <-time.After(time.Second * 10):
			t.Fatal("deleting series took more then 10seconds.")
		case response := <-done:
			So(response.err, ShouldBeNil)
			So(response.defs, ShouldHaveLength, 100000)
		}
	})
}

func TestMixedBranchLeaf(t *testing.T) {
	testWithAndWithoutTagSupport(t, testMixedBranchLeaf)
}

func testMixedBranchLeaf(t *testing.T) {
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

		ix.AddOrUpdate(first, 1)
		Convey("we should be able to add a leaf under another leaf", func() {

			ix.AddOrUpdate(second, 1)
			_, ok := ix.Get(second.Id)
			So(ok, ShouldEqual, true)
			defs := ix.List(1)
			So(len(defs), ShouldEqual, 2)
		})
		Convey("we should be able to add a leaf that collides with an existing branch", func() {

			ix.AddOrUpdate(third, 1)
			_, ok := ix.Get(third.Id)
			So(ok, ShouldEqual, true)
			defs := ix.List(1)
			So(len(defs), ShouldEqual, 3)
		})
	})
}

func TestMixedBranchLeafDelete(t *testing.T) {
	testWithAndWithoutTagSupport(t, testMixedBranchLeafDelete)
}

func testMixedBranchLeafDelete(t *testing.T) {
	ix := New()
	ix.Init()
	series := []*schema.MetricData{
		{
			Name:     "a.b.c",
			Metric:   "a.b.c",
			OrgId:    1,
			Interval: 10,
		},
		{
			Name:     "a.b.c.d",
			Metric:   "a.b.c.d",
			OrgId:    1,
			Interval: 10,
		},
		{
			Name:     "a.b.c2",
			Metric:   "a.b.c2",
			OrgId:    1,
			Interval: 10,
		},
		{
			Name:     "a.b.c2.d.e",
			Metric:   "a.b.c2.d.e",
			OrgId:    1,
			Interval: 10,
		},
		{
			Name:     "a.b.c2.d2.e",
			Metric:   "a.b.c2.d2.e",
			OrgId:    1,
			Interval: 10,
		},
	}
	for _, s := range series {
		s.SetId()
		ix.AddOrUpdate(s, 1)
	}
	Convey("when deleting mixed leaf/branch", t, func() {
		defs, err := ix.Delete(1, "a.b.c")
		So(err, ShouldBeNil)
		So(defs, ShouldHaveLength, 2)
		deletedIds := make([]string, len(defs))
		for i, d := range defs {
			deletedIds[i] = d.Id
		}
		So(series[0].Id, ShouldBeIn, deletedIds)
		So(series[1].Id, ShouldBeIn, deletedIds)
		Convey("series should not be present in the metricDef index", func() {
			_, ok := ix.Get(series[0].Id)
			So(ok, ShouldEqual, false)
			Convey("series should not be present in searches", func() {
				found, err := ix.Find(1, "a.b.c", 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 0)
				found, err = ix.Find(1, "a.b.c.d", 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 0)
			})
		})
	})
	Convey("when deleting from branch", t, func() {
		defs, err := ix.Delete(1, "a.b.c2.d.*")
		So(err, ShouldBeNil)
		So(defs, ShouldHaveLength, 1)
		So(defs[0].Id, ShouldEqual, series[3].Id)

		Convey("deleted series should not be present in the metricDef index", func() {
			_, ok := ix.Get(series[3].Id)
			So(ok, ShouldEqual, false)
			Convey("deleted series should not be present in searches", func() {
				found, err := ix.Find(1, "a.b.c2.*", 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 1)
				found, err = ix.Find(1, "a.b.c2.d", 0)
				So(err, ShouldBeNil)
				So(found, ShouldHaveLength, 0)
			})
		})
	})
}

func TestPruneTaggedSeries(t *testing.T) {
	ix := New()
	ix.Init()

	// add old series
	series := getMetricData(1, 2, 5, 10, "metric.bah", true)
	for _, s := range series {
		s.Time = 1
		s.SetId()
		ix.AddOrUpdate(s, 1)
	}

	// add new series
	series = getMetricData(1, 2, 5, 10, "metric.foo", true)
	for _, s := range series {
		s.Time = 10
		s.SetId()
		ix.AddOrUpdate(s, 1)
	}

	Convey("after populating index", t, func() {
		defs := ix.List(-1)
		So(defs, ShouldHaveLength, 10)
	})

	Convey("When purging old series", t, func() {
		purged, err := ix.Prune(1, time.Unix(2, 0))
		So(err, ShouldBeNil)
		So(purged, ShouldHaveLength, 5)
		nodes, err := ix.FindByTag(1, []string{"name=~metric\\.bah.*", "series_id=~[0-4]"}, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
		nodes, err = ix.FindByTag(1, []string{"name=~metric\\.foo.*", "series_id=~[0-4]"}, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 5)
	})

	Convey("after purge", t, func() {
		defs := ix.List(-1)
		So(defs, ShouldHaveLength, 5)
		data := &schema.MetricData{
			Name:     defs[0].Name,
			Metric:   defs[0].Metric,
			Id:       defs[0].Id,
			Tags:     defs[0].Tags,
			Mtype:    defs[0].Mtype,
			OrgId:    1,
			Interval: 10,
			Time:     100,
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
		Convey("When purging old series", func() {
			purged, err := ix.Prune(1, time.Unix(12, 0))
			So(err, ShouldBeNil)
			So(purged, ShouldHaveLength, 4)
			nodes, err := ix.FindByTag(1, []string{"name=~metric\\.foo.*", "series_id=~[0-4]"}, 0)
			So(err, ShouldBeNil)
			So(nodes, ShouldHaveLength, 1)
		})
	})
}

func TestPruneTaggedSeriesWithCollidingTagSets(t *testing.T) {
	_tagSupport := TagSupport
	defer func() { TagSupport = _tagSupport }()
	TagSupport = true

	ix := New()
	ix.Init()

	series := getMetricData(1, 2, 1, 10, "metric.bah", true)
	serie2 := *series[0]
	series = append(series, &serie2)
	series[0].Interval = 1
	series[1].Interval = 2
	series[0].Time = 1
	series[1].Time = 10
	series[0].SetId()
	series[1].SetId()

	for _, s := range series {
		ix.AddOrUpdate(s, 1)
	}

	Convey("after populating index", t, func() {
		defs := ix.List(1)
		So(defs, ShouldHaveLength, 2)
	})

	findExpressions := []string{"name=" + series[1].Name}
	for _, tag := range series[1].Tags {
		findExpressions = append(findExpressions, tag)
	}

	Convey("When purging old series", t, func() {
		purged, err := ix.Prune(1, time.Unix(2, 0))
		So(err, ShouldBeNil)
		So(purged, ShouldHaveLength, 0)
	})

	Convey("After purge", t, func() {
		nodes, err := ix.FindByTag(1, findExpressions, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 2)
	})

	Convey("When purging newer series", t, func() {
		purged, err := ix.Prune(1, time.Unix(20, 0))
		So(err, ShouldBeNil)
		So(purged, ShouldHaveLength, 2)
	})

	Convey("After purge", t, func() {
		nodes, err := ix.FindByTag(1, findExpressions, 0)
		So(err, ShouldBeNil)
		So(nodes, ShouldHaveLength, 0)
	})
}

func TestPrune(t *testing.T) {
	testWithAndWithoutTagSupport(t, testPrune)
}

func testPrune(t *testing.T) {
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
		data := &schema.MetricData{
			Name:     defs[0].Name,
			Metric:   defs[0].Metric,
			Id:       defs[0].Id,
			OrgId:    1,
			Interval: 30,
			Time:     100,
		}
		data.SetId()
		ix.AddOrUpdate(data, 0)
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

func TestSingleNodeMetric(t *testing.T) {
	ix := New()
	ix.Init()

	data := &schema.MetricData{
		Name:     "node1",
		Metric:   "node1",
		Interval: 10,
		OrgId:    1,
	}
	data.SetId()
	ix.AddOrUpdate(data, 1)
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

func BenchmarkDeletes(b *testing.B) {
	ix := New()
	ix.Init()

	var data *schema.MetricData
	var key string
	for i := 1; i <= b.N; i++ {
		key = fmt.Sprintf("some.metric.%d.%d", i, i)
		data = &schema.MetricData{
			Name:     key,
			Metric:   key,
			OrgId:    1,
			Interval: 10,
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	b.ReportAllocs()
	b.ResetTimer()

	ix.Delete(1, "some.*")
}

func TestMatchSchemaWithTags(t *testing.T) {
	_tagSupport := TagSupport
	_schemas := mdata.Schemas
	defer func() { TagSupport = _tagSupport }()
	defer func() { mdata.Schemas = _schemas }()

	TagSupport = true
	mdata.Schemas = conf.NewSchemas([]conf.Schema{
		{
			Name:       "tag1_is_value3_or_value5",
			Pattern:    regexp.MustCompile(".*;tag1=value[35](;.*|$)"),
			Retentions: conf.Retentions([]conf.Retention{conf.NewRetentionMT(1, 3600*24*1, 600, 2, true)}),
		},
	})

	ix := New()
	ix.Init()

	data := make([]*schema.MetricDefinition, 10)
	archives := make([]idx.Archive, 10)
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("some.id.of.a.metric.%d", i)
		data[i] = &schema.MetricDefinition{
			Name:     name,
			Metric:   name,
			OrgId:    1,
			Interval: 1,
			Tags:     []string{fmt.Sprintf("tag1=value%d", i), "tag2=othervalue"},
		}
		data[i].SetId()
		archives[i] = ix.add(data[i])
	}

	// only those MDs with tag1=value3 or tag1=value5 should get the first schema id
	expectedSchemas := []uint16{1, 1, 1, 0, 1, 0, 1, 1, 1, 1}
	for i := 0; i < 10; i++ {
		if archives[i].SchemaId != expectedSchemas[i] {
			t.Fatalf("Expected schema of archive %d to be %d, but it was %d", i, expectedSchemas[i], archives[i].SchemaId)
		}
	}
}
