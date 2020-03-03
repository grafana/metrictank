package memory

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"
)

var (
	ix                   MemoryIndex
	metaRecordIdx        idx.MetaRecordIdx
	currentIndex         int  // 1 small; 2 large
	currentlyPartitioned bool // was the last call to New() for a partitioned or un-partitioned index.
)

type query struct {
	Pattern         string
	ExpectedResults int
}

type testQuery struct {
	Expressions     []string
	ExpectedResults int
}

type metric struct {
	Name string
	Tags []string
}

var queries = []query{
	//LEAF queries
	{Pattern: "collectd.dc1.host960.disk.disk1.disk_ops.read;dc=dc1;device=disk;direction=read;disk=disk1;host=host960;metric=disk_ops", ExpectedResults: 1},
	{Pattern: "collectd.dc1.host960.disk.disk1.disk_ops.*", ExpectedResults: 2},
	{Pattern: "collectd.*.host960.disk.disk1.disk_ops.read;*", ExpectedResults: 5},
	{Pattern: "collectd.*.host960.disk.disk1.disk_ops.*", ExpectedResults: 10},
	{Pattern: "collectd.d*.host960.disk.disk1.disk_ops.*", ExpectedResults: 10},
	{Pattern: "collectd.[abcd]*.host960.disk.disk1.disk_ops.*", ExpectedResults: 10},
	{Pattern: "collectd.{dc1,dc50}.host960.disk.disk1.disk_ops.*", ExpectedResults: 2},

	{Pattern: "collectd.dc3.host960.cpu.1.idle;cpu=cpu1;dc=dc3;device=cpu;host=host960;metric=idle", ExpectedResults: 1},
	{Pattern: "collectd.dc30.host960.cpu.1.idle;cpu=cpu1;dc=dc30;device=cpu;host=host960;metric=idle", ExpectedResults: 0},
	{Pattern: "collectd.dc3.host960.*.*.idle;*", ExpectedResults: 32},
	{Pattern: "collectd.dc3.host960.*.*.idle;*", ExpectedResults: 32},

	{Pattern: "collectd.dc3.host96[0-9].cpu.1.idle;*", ExpectedResults: 10},
	{Pattern: "collectd.dc30.host96[0-9].cpu.1.idle;*", ExpectedResults: 0},
	{Pattern: "collectd.dc3.host96[0-9].*.*.idle;*", ExpectedResults: 320},
	{Pattern: "collectd.dc3.host96[0-9].*.*.idle;*", ExpectedResults: 320},

	{Pattern: "collectd.{dc1,dc2,dc3}.host960.cpu.1.idle;*", ExpectedResults: 3},
	{Pattern: "collectd.{dc*, a*}.host960.cpu.1.idle;*", ExpectedResults: 5},

	//Branch queries
	{Pattern: "collectd.dc1.host960.*", ExpectedResults: 2},
	{Pattern: "collectd.*.host960.disk.disk1.*", ExpectedResults: 20},
	{Pattern: "collectd.[abcd]*.host960.disk.disk1.*", ExpectedResults: 20},

	{Pattern: "collectd.*.host960.disk.*.*", ExpectedResults: 200},
	{Pattern: "*.dc3.host960.cpu.1.*", ExpectedResults: 8},
	{Pattern: "*.dc3.host96{1,3}.cpu.1.*", ExpectedResults: 16},
	{Pattern: "*.dc3.{host,server}96{1,3}.cpu.1.*", ExpectedResults: 16},

	{Pattern: "*.dc3.{host,server}9[6-9]{1,3}.cpu.1.*", ExpectedResults: 64},
}

var tagQueries = []testQuery{
	// simple matching
	{Expressions: []string{"dc=dc1", "host=host960", "disk=disk1", "metric=disk_ops"}, ExpectedResults: 2},
	{Expressions: []string{"dc=dc3", "host=host960", "disk=disk2", "direction=read"}, ExpectedResults: 4},

	// regular expressions
	{Expressions: []string{"dc=~dc[1-3]", "host=~host3[5-9]{2}", "metric=disk_ops"}, ExpectedResults: 1500},
	{Expressions: []string{"dc=~dc[0-9]", "host=~host97[0-9]", "disk=disk2", "metric=disk_ops"}, ExpectedResults: 100},

	// matching and filtering
	{Expressions: []string{"dc=dc1", "host=host666", "cpu=cpu12", "device=cpu", "metric!=softirq"}, ExpectedResults: 7},
	{Expressions: []string{"dc=dc1", "host=host966", "cpu!=cpu12", "device!=disk", "metric!=softirq"}, ExpectedResults: 217},

	// matching and filtering by regular expressions
	{Expressions: []string{"dc=dc1", "host=host666", "cpu!=~cpu[0-9]{2}", "device!=~d.*"}, ExpectedResults: 80},
	{Expressions: []string{"dc=dc1", "host!=~host10[0-9]{2}", "device!=~c.*"}, ExpectedResults: 4000},
}

func cpuMetrics(dcCount, hostCount, hostOffset, cpuCount int, prefix string) []metric {
	var series []metric
	for dc := 0; dc < dcCount; dc++ {
		for host := hostOffset; host < hostCount+hostOffset; host++ {
			for cpu := 0; cpu < cpuCount; cpu++ {
				p := prefix + ".dc" + strconv.Itoa(dc) + ".host" + strconv.Itoa(host) + ".cpu." + strconv.Itoa(cpu)
				for _, m := range []string{"idle", "interrupt", "nice", "softirq", "steal", "system", "user", "wait"} {
					series = append(series, metric{
						Name: p + "." + m,
						Tags: []string{
							"cpu=cpu" + strconv.Itoa(cpu),
							"dc=dc" + strconv.Itoa(dc),
							"device=cpu",
							"host=host" + strconv.Itoa(host),
							"metric=" + m,
						},
					})
				}
			}
		}
	}
	return series
}

func diskMetrics(dcCount, hostCount, hostOffset, diskCount int, prefix string) []metric {
	var series []metric
	for dc := 0; dc < dcCount; dc++ {
		for host := hostOffset; host < hostCount+hostOffset; host++ {
			for disk := 0; disk < diskCount; disk++ {
				p := prefix + ".dc" + strconv.Itoa(dc) + ".host" + strconv.Itoa(host) + ".disk.disk" + strconv.Itoa(disk)
				for _, m := range []string{"disk_merged", "disk_octets", "disk_ops", "disk_time"} {
					series = append(series, metric{
						Name: p + "." + m + ".read",
						Tags: []string{
							"dc=dc" + strconv.Itoa(dc),
							"device=disk",
							"direction=read",
							"disk=disk" + strconv.Itoa(disk),
							"host=host" + strconv.Itoa(host),
							"metric=" + m,
						},
					})
					series = append(series, metric{
						Name: p + "." + m + ".write",
						Tags: []string{
							"dc=dc" + strconv.Itoa(dc),
							"device=disk",
							"direction=write",
							"disk=disk" + strconv.Itoa(disk),
							"host=host" + strconv.Itoa(host),
							"metric=" + m,
						},
					})
				}
			}
		}
	}
	return series
}

func TestMain(m *testing.M) {
	defer func(t bool) { TagSupport = t }(TagSupport)
	TagSupport = true
	TagQueryWorkers = 5
	matchCacheSize = 1000
	tagquery.MatchCacheSize = 1000
	// we dont need info logs in the test output
	log.SetLevel(log.ErrorLevel)
	os.Exit(m.Run())
}

func InitSmallIndex() {
	// if the current index is not the small index then initialize it
	if currentIndex != 1 || currentlyPartitioned != Partitioned {
		if ix != nil {
			ix.Stop()
		}
		ix = nil

		// run GC because we only get 4G on CircleCI
		runtime.GC()
		cluster.Manager.SetPartitions([]int32{0, 1})
		partitionCount = 2
		currentlyPartitioned = Partitioned
		ix = New()
		ix.Init()
		metaRecordIdx = ix

		currentIndex = 1
	} else {
		ix.PurgeFindCache()
		ix.Init()
		return
	}

	var data *schema.MetricData

	for i, series := range cpuMetrics(5, 100, 0, 32, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
			Time:     int64(i + 100),
		}
		data.SetId()
		mkey, _ := schema.MKeyFromString(data.Id)
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
	for i, series := range diskMetrics(5, 100, 0, 10, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
			Time:     int64(i + 100),
		}
		data.SetId()
		mkey, _ := schema.MKeyFromString(data.Id)
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
}

func InitLargeIndex() {
	// if the current index is not the large index then initialize it
	if currentIndex != 2 || currentlyPartitioned != Partitioned {
		if ix != nil {
			ix.Stop()
		}
		ix = nil

		// run GC because we only get 4G on CircleCI
		runtime.GC()
		cluster.Manager.SetPartitions([]int32{0, 1, 2, 3, 4, 5, 6, 7})
		partitionCount = 8
		currentlyPartitioned = Partitioned
		ix = New()
		ix.Init()

		currentIndex = 2
	} else {
		ix.PurgeFindCache()
		ix.Init()
		return
	}

	var data *schema.MetricData
	for i, series := range cpuMetrics(5, 1000, 0, 32, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
			Time:     int64(i + 100),
		}
		data.SetId()
		mkey, _ := schema.MKeyFromString(data.Id)
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
	for i, series := range diskMetrics(5, 1000, 0, 10, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
			Time:     int64(i + 100),
		}
		data.SetId()
		mkey, _ := schema.MKeyFromString(data.Id)
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
	// orgId 1 has 1,680,000 series

	for i, series := range cpuMetrics(5, 100, 950, 32, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    2,
			Time:     int64(i + 100),
		}
		data.SetId()
		mkey, _ := schema.MKeyFromString(data.Id)
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
	for i, series := range diskMetrics(5, 100, 950, 10, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    2,
			Time:     int64(i + 100),
		}
		data.SetId()
		mkey, _ := schema.MKeyFromString(data.Id)
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}
	//orgId 2 has 168,000 mertics
}

func getMetaTagEnrichers(t testing.TB, ix MemoryIndex) map[uint32][]*metaTagEnricher {
	t.Helper()

	res := make(map[uint32][]*metaTagEnricher)
	switch concrete := ix.(type) {
	case *PartitionedMemoryIdx:
		for _, unpartitionedIdx := range concrete.Partition {
			for orgId, enricher := range unpartitionedIdx.metaTagEnricher {
				res[orgId] = append(res[orgId], enricher)
			}
		}
	case *UnpartitionedMemoryIdx:
		for orgId, enricher := range concrete.metaTagEnricher {
			res[orgId] = append(res[orgId], enricher)
		}
	default:
		t.Fatalf("Invalid index object")
	}

	return res
}

// waitForMetaTagEnrichers obtains all enrichers in existence, across all orgs, and uses
// their .wait() function to wait for each of them. this function blocks until the whole
// enricher queue has been processed up to the point when it was called
func waitForMetaTagEnrichers(t testing.TB, ix MemoryIndex) {
	enrichersByOrg := getMetaTagEnrichers(t, ix)
	for _, enrichers := range enrichersByOrg {
		for _, enricher := range enrichers {
			// stop waits until the queue has been fully consumed
			enricher.stop()
			enricher.start()
		}
	}
}

func queryAndCompareTagValues(t *testing.T, key, filter string, expected map[string]uint64) {
	t.Helper()

	values := ix.TagDetails(1, key, regexp.MustCompile(filter))
	if len(values) != len(expected) {
		t.Fatalf("Expected %d values, but got %d", len(expected), len(values))
	}

	for ev, ec := range expected {
		found := false
		for v, c := range values {
			if ev == v {
				found = true
				if ec != c {
					t.Fatalf("Expected count %d for %s, but got %d", ec, ev, c)
				}
			}
		}
		if !found {
			t.Fatalf("Expected value %s, but did not find it", ev)
		}
	}

}

func TestTagDetailsWithoutFilter(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagDetailsWithoutFilter)(t)
}

func testTagDetailsWithoutFilter(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	expected := make(map[string]uint64)
	expected["dc0"] = 33600
	expected["dc1"] = 33600
	expected["dc2"] = 33600
	expected["dc3"] = 33600
	expected["dc4"] = 33600
	queryAndCompareTagValues(t, "dc", "", expected)
}

func TestTagDetailsWithFilter(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagDetailsWithFilter)(t)
}

func testTagDetailsWithFilter(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	expected := make(map[string]uint64)
	expected["dc3"] = 33600
	expected["dc4"] = 33600

	queryAndCompareTagValues(t, "dc", ".+[3-9]{1}$", expected)
}

func TestTagDetailsWithMetaTagSupportWithoutFilter(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	withAndWithoutPartitonedIndex(testTagDetailsWithMetaTagSupportWithoutFilter)(t)
}

func testTagDetailsWithMetaTagSupportWithoutFilter(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	metaRecordExpression, err := tagquery.ParseExpression("dc=~dc[0-9]+")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}

	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "dc", Value: "all"}},
		Expressions: tagquery.Expressions{metaRecordExpression},
	})

	waitForMetaTagEnrichers(t, ix)

	expected := make(map[string]uint64)
	expected["all"] = 168000
	expected["dc0"] = 33600
	expected["dc1"] = 33600
	expected["dc2"] = 33600
	expected["dc3"] = 33600
	expected["dc4"] = 33600
	queryAndCompareTagValues(t, "dc", "", expected)
}

func TestTagDetailsWithMetaTagSupportWithFilter(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	withAndWithoutPartitonedIndex(testTagDetailsWithMetaTagSupportWithFilter)(t)
}

func testTagDetailsWithMetaTagSupportWithFilter(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	metaRecordExpression, err := tagquery.ParseExpression("dc=dc0")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}

	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "dc", Value: "dc5"}},
		Expressions: tagquery.Expressions{metaRecordExpression},
	})

	waitForMetaTagEnrichers(t, ix)

	expected := make(map[string]uint64)
	expected["dc3"] = 33600
	expected["dc4"] = 33600
	expected["dc5"] = 33600
	queryAndCompareTagValues(t, "dc", ".+[3-9]{1}$", expected)
}

func queryAndCompareTagKeys(t testing.TB, filter string, expected []string) {
	t.Helper()
	values := ix.Tags(1, regexp.MustCompile(filter))
	if len(values) != len(expected) {
		t.Fatalf("Expected %d values, but got %d", len(expected), len(values))
	}

	sort.Strings(expected)

	// reflect.DeepEqual treats nil & []string{} as not equal
	if len(values) == 0 {
		values = nil
	}
	if len(expected) == 0 {
		expected = nil
	}

	if !reflect.DeepEqual(values, expected) {
		t.Fatalf("Expected values:\n%+v\nGot:\n%+v", expected, values)
	}
}

func TestTagKeysWithoutFilters(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagKeysWithoutFilters)(t)
}

func testTagKeysWithoutFilters(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	expected := []string{"dc", "host", "device", "cpu", "metric", "direction", "disk", "name"}
	queryAndCompareTagKeys(t, "", expected)
}

func TestTagKeysWithFilter(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagKeysWithFilter)(t)
}

func testTagKeysWithFilter(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	expected := []string{"dc", "device", "disk", "direction"}
	queryAndCompareTagKeys(t, "d", expected)

	expected = []string{"direction", "disk"}
	queryAndCompareTagKeys(t, "di", expected)
}

func TestTagKeysWithMetaTagSupportWithFilter(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	withAndWithoutPartitonedIndex(testTagKeysWithMetaTagSupportWithFilter)(t)
}

func testTagKeysWithMetaTagSupportWithFilter(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	metaRecordExpression, err := tagquery.ParseExpression("direction=read")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}

	orgId := uint32(1)
	metaRecordIdx.MetaTagRecordUpsert(orgId, tagquery.MetaTagRecord{
		MetaTags: tagquery.Tags{
			{Key: "directionMeta", Value: "read"},
			{Key: "directionMeta2", Value: "read"},
		},
		Expressions: tagquery.Expressions{metaRecordExpression},
	})

	waitForMetaTagEnrichers(t, ix)

	expected := []string{"dc", "device", "disk", "direction", "directionMeta", "directionMeta2"}
	queryAndCompareTagKeys(t, "d", expected)

	expected = []string{"direction", "disk", "directionMeta", "directionMeta2"}
	queryAndCompareTagKeys(t, "di", expected)
}

func TestTagKeysWithMetaTagSupportWithoutFilters(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	withAndWithoutPartitonedIndex(testTagKeysWithMetaTagSupportWithoutFilters)(t)
}

func testTagKeysWithMetaTagSupportWithoutFilters(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	metaRecordExpression, err := tagquery.ParseExpression("name!=")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}

	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "all", Value: "metrics"}},
		Expressions: tagquery.Expressions{metaRecordExpression},
	})

	waitForMetaTagEnrichers(t, ix)

	expected := []string{"all", "dc", "host", "device", "cpu", "metric", "direction", "disk", "name"}
	queryAndCompareTagKeys(t, "", expected)
}

func TestTagSorting(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagSorting)(t)
}

func testTagSorting(t *testing.T) {
	index := New()
	defer index.Stop()
	index.Init()

	md1 := &schema.MetricData{
		Name:     "name1",
		Tags:     []string{},
		Interval: 10,
		OrgId:    1,
		Time:     int64(123),
	}
	md1.SetId()
	mkey, _ := schema.MKeyFromString(md1.Id)

	// set out of order tags after SetId (because that would sort it)
	// e.g. mimic the case where somebody sent us a MD with an id already set and out-of-order tags
	md1.Tags = []string{"d=a", "b=a", "c=a", "a=a", "e=a"}
	index.AddOrUpdate(mkey, md1, getPartition(md1))

	query, err := tagquery.NewQueryFromStrings([]string{"b=a"}, 0)
	if err != nil {
		t.Fatalf("Unexpected error returned when parsing query: %s", err)
	}
	res := index.FindByTag(1, query)
	if len(res) != 1 {
		t.Fatalf("Expected exactly 1 result, got %d: %+v", len(res), res)
	}
	expected := "name1;a=a;b=a;c=a;d=a;e=a"
	if res[0].Path != expected {
		t.Fatalf("Wrong metric name returned.\nExpected: %s\nGot: %s\n", expected, res[0].Path)
	}

	md2 := []schema.MetricDefinition{
		{
			Name:       "name2",
			Tags:       []string{},
			Interval:   10,
			OrgId:      1,
			LastUpdate: int64(123),
		},
	}
	md2[0].SetId()

	// set out of order tags after SetId (because that would sort it)
	// e.g. mimic the case where somebody sent us a MD with an id already set and out-of-order tags
	md2[0].Tags = []string{"5=a", "1=a", "2=a", "4=a", "3=a"}
	index.LoadPartition(getPartitionFromName("name2"), md2)

	query, err = tagquery.NewQueryFromStrings([]string{"3=a"}, 0)
	if err != nil {
		t.Fatalf("Unexpected error when parsing query: %s", err)
	}
	res = index.FindByTag(1, query)
	if len(res) != 1 {
		t.Fatalf("Expected exactly 1 result, got %d: %+v", len(res), res)
	}
	expected = "name2;1=a;2=a;3=a;4=a;5=a"
	if res[0].Path != expected {
		t.Fatalf("Wrong metric name returned.\nExpected: %s\nGot: %s\n", expected, res[0].Path)
	}
}

func TestAutoCompleteTags(t *testing.T) {
	withAndWithoutPartitonedIndex(testAutoCompleteTags)(t)
}

func testAutoCompleteTags(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	type testCase struct {
		prefix string
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			prefix: "ho",
			limit:  100,
			expRes: []string{"host"},
		}, {
			prefix: "host",
			limit:  100,
			expRes: []string{"host"},
		}, {
			prefix: "n",
			limit:  100,
			expRes: []string{"name"},
		}, {
			prefix: "",
			limit:  100,
			expRes: []string{"cpu", "dc", "device", "direction", "disk", "host", "metric", "name"},
		},
	}

	for i, tc := range testCases {
		autoCompleteTagsAndCompare(t, i, tc.prefix, tc.limit, tc.expRes)
	}
}

func TestAutoCompleteTagsWithMetaTagSupport(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	withAndWithoutPartitonedIndex(testAutoCompleteTagsWithMetaTagSupport)(t)
}

func testAutoCompleteTagsWithMetaTagSupport(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	metaRecordExpression1, err := tagquery.ParseExpression("host=~.+")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}
	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "host2", Value: "all"}},
		Expressions: tagquery.Expressions{metaRecordExpression1},
	})

	metaRecordExpression2, err := tagquery.ParseExpression("name=~.+")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}
	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "all", Value: "metrics"}},
		Expressions: tagquery.Expressions{metaRecordExpression2},
	})

	waitForMetaTagEnrichers(t, ix)

	type testCase struct {
		prefix string
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			prefix: "ho",
			limit:  100,
			expRes: []string{"host", "host2"},
		}, {
			prefix: "host2",
			limit:  100,
			expRes: []string{"host2"},
		}, {
			prefix: "n",
			limit:  100,
			expRes: []string{"name"},
		}, {
			prefix: "",
			limit:  100,
			expRes: []string{"all", "cpu", "dc", "device", "direction", "disk", "host", "host2", "metric", "name"},
		}, {
			prefix: "",
			limit:  2,
			expRes: []string{"all", "cpu"},
		}, {
			prefix: "a",
			limit:  100,
			expRes: []string{"all"},
		},
	}

	for i, tc := range testCases {
		autoCompleteTagsAndCompare(t, i, tc.prefix, tc.limit, tc.expRes)
	}
}

func autoCompleteTagsAndCompare(t testing.TB, tcIdx int, prefix string, limit uint, expRes []string) {
	t.Helper()

	res := ix.FindTags(1, prefix, limit)

	if len(res) != len(expRes) {
		t.Fatalf("TC %d: Wrong result, Expected:\n%s\nGot:\n%s\n", tcIdx, expRes, res)
	}

	for i := range res {
		if expRes[i] != res[i] {
			t.Fatalf("TC %d: Wrong result, Expected:\n%s\nGot:\n%s\n", tcIdx, expRes, res)
		}
	}
}

func TestAutoCompleteTagsWithQuery(t *testing.T) {
	withAndWithoutPartitonedIndex(testAutoCompleteTagsWithQuery)(t)
}

func testAutoCompleteTagsWithQuery(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	type testCase struct {
		prefix string
		expr   []string
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			prefix: "di",
			expr:   []string{"direction=write", "host=host90"},
			limit:  100,
			expRes: []string{"direction", "disk"},
		}, {
			prefix: "di",
			expr:   []string{"direction=write", "host=host90", "device=cpu"},
			limit:  100,
			expRes: []string{},
		}, {
			prefix: "",
			expr:   []string{"direction=write", "host=host90"},
			limit:  100,
			expRes: []string{"dc", "device", "direction", "disk", "host", "metric", "name"},
		}, {
			prefix: "",
			expr:   []string{"direction=write", "host=host90"},
			limit:  3,
			expRes: []string{"dc", "device", "direction"},
		},
	}

	for i, tc := range testCases {
		autoCompleteTagsWithQueryAndCompare(t, i, tc.prefix, tc.expr, tc.limit, tc.expRes)
	}
}

func TestAutoCompleteTagsWithQueryWithMetaTagSupport(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	withAndWithoutPartitonedIndex(testAutoCompleteTagsWithQueryWithMetaTagSupport)(t)
}

func testAutoCompleteTagsWithQueryWithMetaTagSupport(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	metaRecordExpression, err := tagquery.ParseExpression("host=~.+")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}

	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "another", Value: "tag"}, tagquery.Tag{Key: "meta", Value: "tag"}},
		Expressions: tagquery.Expressions{metaRecordExpression},
	})

	waitForMetaTagEnrichers(t, ix)

	type testCase struct {
		prefix string
		expr   []string
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			prefix: "me",
			expr:   []string{"direction=write"},
			limit:  100,
			expRes: []string{"meta", "metric"},
		}, {
			prefix: "",
			expr:   []string{"direction=write", "host=host90"},
			limit:  100,
			expRes: []string{"another", "dc", "device", "direction", "disk", "host", "meta", "metric", "name"},
		}, {
			prefix: "",
			expr:   []string{"direction=write", "host=host90"},
			limit:  8,
			expRes: []string{"another", "dc", "device", "direction", "disk", "host", "meta", "metric"},
		},
	}

	for i, tc := range testCases {
		autoCompleteTagsWithQueryAndCompare(t, i, tc.prefix, tc.expr, tc.limit, tc.expRes)
	}
}

func autoCompleteTagsWithQueryAndCompare(t testing.TB, tcIdx int, prefix string, expr []string, limit uint, expRes []string) {
	t.Helper()

	query, err := tagquery.NewQueryFromStrings(expr, 0)
	if err != nil {
		t.Fatalf("Error when instantiating query: %s", err)
	}
	res := ix.FindTagsWithQuery(1, prefix, query, limit)

	if len(res) != len(expRes) {
		t.Fatalf("TC %d: Wrong result, Expected:\n%s\nGot:\n%s\n", tcIdx, expRes, res)
	}

	for i := range res {
		if expRes[i] != res[i] {
			t.Fatalf("TC %d: Wrong result, Expected:\n%s\nGot:\n%s\n", tcIdx, expRes, res)
		}
	}
}

func TestAutoCompleteTagValues(t *testing.T) {
	withAndWithoutPartitonedIndex(testAutoCompleteTagValues)(t)
}

func testAutoCompleteTagValues(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	type testCase struct {
		tag    string
		prefix string
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			tag:    "device",
			prefix: "",
			limit:  100,
			expRes: []string{"cpu", "disk"},
		}, {
			tag:    "metric",
			prefix: "disk_o",
			limit:  100,
			expRes: []string{"disk_octets", "disk_ops"},
		},
		{
			tag:    "metric",
			prefix: "disk_o",
			limit:  1,
			expRes: []string{"disk_octets"},
		},
	}

	for _, tc := range testCases {
		autoCompleteTagValuesAndCompare(t, tc.tag, tc.prefix, tc.limit, tc.expRes)
	}
}

func TestAutoCompleteTagValuesWithMetaTagSupport(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	withAndWithoutPartitonedIndex(testAutoCompleteTagValuesWithMetaTagSupport)(t)
}

func testAutoCompleteTagValuesWithMetaTagSupport(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	metaRecordExpression1, err := tagquery.ParseExpression("metric=~.+")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}
	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "metric", Value: "all"}},
		Expressions: tagquery.Expressions{metaRecordExpression1},
	})

	metaRecordExpression2, err := tagquery.ParseExpression("metric=~disk_.+")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}
	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "metric", Value: "disk_all"}},
		Expressions: tagquery.Expressions{metaRecordExpression2},
	})

	metaRecordExpression3, err := tagquery.ParseExpression("name!=")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}
	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "meta1", Value: "all_metrics"}},
		Expressions: tagquery.Expressions{metaRecordExpression3},
	})

	waitForMetaTagEnrichers(t, ix)

	type testCase struct {
		tag    string
		prefix string
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			tag:    "metric",
			prefix: "",
			limit:  100,
			expRes: []string{"all", "disk_all", "disk_merged", "disk_octets", "disk_ops", "disk_time", "idle", "interrupt", "nice", "softirq", "steal", "system", "user", "wait"},
		}, {
			tag:    "metric",
			prefix: "disk_",
			limit:  100,
			expRes: []string{"disk_all", "disk_merged", "disk_octets", "disk_ops", "disk_time"},
		}, {
			tag:    "metric",
			prefix: "disk_",
			limit:  3,
			expRes: []string{"disk_all", "disk_merged", "disk_octets"},
		}, {
			tag:    "meta1",
			prefix: "",
			limit:  100,
			expRes: []string{"all_metrics"},
		},
	}

	for _, tc := range testCases {
		autoCompleteTagValuesAndCompare(t, tc.tag, tc.prefix, tc.limit, tc.expRes)
	}
}

func autoCompleteTagValuesAndCompare(t testing.TB, tag, prefix string, limit uint, expRes []string) {
	t.Helper()

	res := ix.FindTagValues(1, tag, prefix, limit)

	if len(res) != len(expRes) {
		t.Fatalf("Wrong result, Expected:\n%s\nGot:\n%s\n", expRes, res)
	}

	sort.Strings(expRes)
	sort.Strings(res)
	for i := range res {
		if expRes[i] != res[i] {
			t.Fatalf("Wrong result, Expected:\n%s\nGot:\n%s\n", expRes, res)
		}
	}
}

func TestAutoCompleteTagValuesWithQuery(t *testing.T) {
	withAndWithoutPartitonedIndex(testAutoCompleteTagValuesWithQuery)(t)
}

func testAutoCompleteTagValuesWithQuery(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	type testCase struct {
		tag    string
		prefix string
		expr   []string
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			tag:    "host",
			prefix: "host9",
			expr:   []string{"direction=write"},
			limit:  100,
			expRes: []string{"host9", "host90", "host91", "host92", "host93", "host94", "host95", "host96", "host97", "host98", "host99"},
		}, {
			tag:    "host",
			prefix: "host9",
			expr:   []string{"direction=write"},
			limit:  5,
			expRes: []string{"host9", "host90", "host91", "host92", "host93"},
		}, {
			tag:    "direction",
			prefix: "w",
			expr:   []string{"device=disk"},
			limit:  100,
			expRes: []string{"write"},
		}, {
			tag:    "direction",
			prefix: "w",
			expr:   []string{"device=cpu"},
			limit:  100,
			expRes: []string{},
		}, {
			tag:    "device",
			prefix: "",
			expr:   []string{"disk=~disk[4-5]{1}"},
			limit:  100,
			expRes: []string{"disk"},
		},
	}

	for i, tc := range testCases {
		autoCompleteTagValuesWithQueryAndCompare(t, i, tc.tag, tc.prefix, tc.expr, tc.limit, tc.expRes)
	}
}

func TestAutoCompleteTagValuesWithQueryWithMetaTagSupport(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	withAndWithoutPartitonedIndex(testAutoCompleteTagValuesWithQueryWithMetaTagSupport)(t)
}

func testAutoCompleteTagValuesWithQueryWithMetaTagSupport(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	metaRecordExpression1, err := tagquery.ParseExpression("name=~.*\\.cpu\\..*")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}
	// since "direction" is a tag in the metric tag index, we don't
	// include values of the meta tag index in the autocomplete results
	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "direction", Value: "none"}},
		Expressions: tagquery.Expressions{metaRecordExpression1},
	})

	metaRecordExpressions2, err := tagquery.ParseExpressions([]string{"direction=~.+"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{{Key: "has_direction", Value: "true"}, {Key: "has_direction", Value: "yes"}},
		Expressions: metaRecordExpressions2,
	})

	metaRecordExpression3, err := tagquery.ParseExpression("name!=")
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}
	metaRecordIdx.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags:    tagquery.Tags{tagquery.Tag{Key: "all", Value: "metrics"}},
		Expressions: tagquery.Expressions{metaRecordExpression3},
	})

	waitForMetaTagEnrichers(t, ix)

	type testCase struct {
		tag    string
		prefix string
		expr   []string
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			tag:    "direction",
			prefix: "",
			expr:   []string{"device=disk"},
			limit:  100,
			expRes: []string{"read", "write"},
		}, {
			tag:    "direction",
			prefix: "",
			expr:   []string{"host=~.+"},
			limit:  100,
			expRes: []string{"none", "read", "write"},
		}, {
			tag:    "direction",
			prefix: "wr",
			expr:   []string{"host=~.+"},
			limit:  100,
			expRes: []string{"write"},
		}, {
			tag:    "direction",
			prefix: "no",
			expr:   []string{"host=~.+"},
			limit:  100,
			expRes: []string{"none"},
		}, {
			tag:    "direction",
			prefix: "",
			expr:   []string{"host=~.+"},
			limit:  2,
			expRes: []string{"none", "read"},
		}, {
			tag:    "all",
			prefix: "",
			expr:   []string{"__tag=name"},
			limit:  100,
			expRes: []string{"metrics"},
		}, {
			tag:    "has_direction",
			prefix: "",
			expr:   []string{"name=~.+"},
			limit:  100,
			expRes: []string{"true", "yes"},
		}, {
			tag:    "has_direction",
			prefix: "tr",
			expr:   []string{"name=~.+"},
			limit:  100,
			expRes: []string{"true"},
		}, {
			tag:    "has_direction",
			prefix: "",
			expr:   []string{"name=~.+"},
			limit:  1,
			expRes: []string{"true"},
		},
	}

	for i, tc := range testCases {
		autoCompleteTagValuesWithQueryAndCompare(t, i, tc.tag, tc.prefix, tc.expr, tc.limit, tc.expRes)
	}
}

func autoCompleteTagValuesWithQueryAndCompare(t testing.TB, tc int, tag, prefix string, expr []string, limit uint, expRes []string) {
	t.Helper()

	query, err := tagquery.NewQueryFromStrings(expr, 0)
	if err != nil {
		t.Fatalf("TC %d: Unexpected error when instantiating query: %s", tc, err)
	}
	res := ix.FindTagValuesWithQuery(1, tag, prefix, query, limit)

	if len(res) != len(expRes) {
		t.Fatalf("TC %d: Wrong result, Expected:\n%s\nGot:\n%s\n", tc, expRes, res)
	}

	sort.Strings(expRes)
	sort.Strings(res)
	for i := range res {
		if expRes[i] != res[i] {
			t.Fatalf("TC %d: Wrong result, Expected:\n%s\nGot:\n%s\n", tc, expRes, res)
		}
	}
}

func BenchmarkTagDetailsWithoutFilter(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagDetailsWithoutFilter)(b)
}

func benchmarkTagDetailsWithoutFilter(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	b.ReportAllocs()
	b.ResetTimer()

	expectedCount := uint64(100000)
	for n := 0; n < b.N; n++ {
		val := ix.TagDetails(1, "metric", regexp.MustCompile(""))
		if val["disk_ops"] != expectedCount {
			b.Fatalf("Expected count %d, but got %d: %+v", expectedCount, val["disk_ops"], val)
		}
	}
}

func BenchmarkTagDetailsWithFilter(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagDetailsWithFilter)(b)
}

func benchmarkTagDetailsWithFilter(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	filters := []string{"i", ".+rr", ".+t", "interrupt"}
	expectedCounts := []uint64{158762, 158750, 158737, 158725}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		q := n % len(filters)
		val := ix.TagDetails(1, "metric", regexp.MustCompile(filters[q]))
		if val["interrupt"] != expectedCounts[q] {
			b.Fatalf("Expected count %d, but got %d: %+v", expectedCounts[q], val["interrupt"], val)
		}
	}
}

func BenchmarkTagsWithFilter(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagsWithFilter)(b)
}

func benchmarkTagsWithFilter(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()
	filters := []string{"d", "di", "^c"}
	expected := [][]string{
		{"dc", "device", "direction", "disk"},
		{"direction", "disk"},
		{"cpu"},
	}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		q := n % len(filters)
		queryAndCompareTagKeys(b, filters[q], expected[q])
	}
}

func BenchmarkTagsWithoutFilter(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagsWithoutFilter)(b)
}

func benchmarkTagsWithoutFilter(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()
	expected := []string{"dc", "device", "direction", "disk", "cpu", "metric", "name", "host"}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		queryAndCompareTagKeys(b, "", expected)
	}
}

func ixFind(b *testing.B, org uint32, q int) {
	b.Helper()

	nodes, err := ix.Find(org, queries[q].Pattern, 0)
	if err != nil {
		panic(err)
	}
	if len(nodes) != queries[q].ExpectedResults {
		for _, n := range nodes {
			b.Log(n.Path)
		}
		b.Errorf("%s expected %d got %d results instead", queries[q].Pattern, queries[q].ExpectedResults, len(nodes))
	}
}

func BenchmarkFind(b *testing.B) {
	_tagSupport := TagSupport
	defer func() { TagSupport = _tagSupport }()
	TagSupport = false
	benchWithAndWithoutPartitonedIndex(benchmarkFind)(b)
}

func benchmarkFind(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()
	queryCount := len(queries)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		org := uint32((n % 2) + 1)
		ixFind(b, org, q)
	}
}

type testQ struct {
	q   int
	org uint32
}

func BenchmarkConcurrent4Find(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkConcurrent4Find)(b)
}

func benchmarkConcurrent4Find(b *testing.B) {
	_tagSupport := TagSupport
	TagSupport = false
	defer func() {
		TagSupport = _tagSupport
	}()

	InitLargeIndex()
	defer ix.Stop()
	queryCount := len(queries)
	var wg sync.WaitGroup
	ch := make(chan testQ)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			for q := range ch {
				ixFind(b, q.org, q.q)
			}
			wg.Done()
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		org := uint32((n % 2) + 1)
		ch <- testQ{q: q, org: org}
	}
	close(ch)
	wg.Wait()
}

func BenchmarkConcurrent8Find(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkConcurrent8Find)(b)
}

func benchmarkConcurrent8Find(b *testing.B) {
	_tagSupport := TagSupport
	TagSupport = false
	defer func() {
		TagSupport = _tagSupport
	}()
	InitLargeIndex()
	defer ix.Stop()
	queryCount := len(queries)
	var wg sync.WaitGroup
	ch := make(chan testQ)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			for q := range ch {
				ixFind(b, q.org, q.q)
			}
			wg.Done()
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		org := uint32((n % 2) + 1)
		ch <- testQ{q: q, org: org}
	}
	close(ch)
	wg.Wait()
}

func BenchmarkConcurrentInsertFind(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkConcurrentInsertFind)(b)
}

func benchmarkConcurrentInsertFind(b *testing.B) {
	_tagSupport := TagSupport
	TagSupport = false
	defer func() {
		TagSupport = _tagSupport
	}()
	findCacheInvalidateQueueSize = 10
	InitLargeIndex()
	defer ix.Stop()
	queryCount := len(queries)
	var wg sync.WaitGroup
	ch := make(chan testQ)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			for q := range ch {
				ixFind(b, q.org, q.q)
			}
			wg.Done()
		}()
	}

	// run a single thread adding a new series every few milliseconds
	done := make(chan struct{})
	go func() {
		i := 0
		var t int64
		var data *schema.MetricData
		for {
			select {
			case <-done:
				//b.Logf("spent %s adding %d items to index.", time.Duration(t).String(), i)
				return
			default:
				data = &schema.MetricData{
					Name:     fmt.Sprintf("benchmark.foo.%d", i),
					Tags:     []string{},
					Interval: 10,
					OrgId:    1,
					Time:     int64(i + 100),
				}
				data.SetId()
				mkey, _ := schema.MKeyFromString(data.Id)
				pre := time.Now()
				ix.AddOrUpdate(mkey, data, getPartition(data))
				t += time.Since(pre).Nanoseconds()
				time.Sleep(time.Millisecond * 5)
				i++
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		org := uint32((n % 2) + 1)
		ch <- testQ{q: q, org: org}
	}
	close(ch)
	wg.Wait()
	close(done)
}

func ixFindByTag(b *testing.B, org uint32, q int) {
	query, err := tagquery.NewQueryFromStrings(tagQueries[q].Expressions, 0)
	if err != nil {
		panic(err)
	}
	series := ix.FindByTag(org, query)
	if len(series) != tagQueries[q].ExpectedResults {
		for _, s := range series {
			a, _ := ix.Get(s.Defs[0].Id)
			b.Log(a.Tags)
		}
		b.Fatalf("%+v expected %d got %d results instead", tagQueries[q].Expressions, tagQueries[q].ExpectedResults, len(series))
	}
}

func BenchmarkTagFindSimpleIntersect(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagFindSimpleIntersect)(b)
}

func benchmarkTagFindSimpleIntersect(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % 2
		org := uint32((n % 2) + 1)
		ixFindByTag(b, org, q)
	}
}

func BenchmarkTagFindRegexIntersect(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagFindRegexIntersect)(b)
}

func benchmarkTagFindRegexIntersect(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := (n % 2) + 2
		org := uint32((n % 2) + 1)
		ixFindByTag(b, org, q)
	}
}

func BenchmarkTagFindMatchingAndFiltering(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagFindMatchingAndFiltering)(b)
}

func benchmarkTagFindMatchingAndFiltering(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := (n % 2) + 4
		org := uint32((n % 2) + 1)
		ixFindByTag(b, org, q)
	}
}

func BenchmarkTagFindMatchingAndFilteringWithRegex(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagFindMatchingAndFilteringWithRegex)(b)
}

func benchmarkTagFindMatchingAndFilteringWithRegex(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := (n % 2) + 6
		org := uint32((n % 2) + 1)
		ixFindByTag(b, org, q)
	}
}

func permutations(lst []string) [][]string {
	if len(lst) == 0 {
		return [][]string{}
	}
	if len(lst) == 1 {
		return [][]string{lst}
	}

	l := make([][]string, 0)
	for i, e1 := range lst {
		remaining := make([]string, 0, len(lst)-1)
		for j, e2 := range lst {
			if i == j {
				continue
			}
			remaining = append(remaining, e2)
		}
		for _, perms := range permutations(remaining) {
			perm := []string{e1}
			for _, p := range perms {
				perm = append(perm, p)
			}
			l = append(l, perm)
		}
	}
	return l
}

// since that's going through a lot of permutations it needs an increased
// benchtime to be meaningful. f.e. on my laptop i'm using -benchtime=1m, which
// is enough for it to go through all the 6! permutations
func BenchmarkTagQueryFilterAndIntersect(b *testing.B) {
	benchWithAndWithoutMetaTagSupport(benchWithAndWithoutPartitonedIndex(benchmarkTagQueryFilterAndIntersect))(b)
}

func benchmarkTagQueryFilterAndIntersect(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	queries := make([]testQuery, 0)
	for _, expressions := range permutations([]string{"direction!=~read", "device!=", "host=~host9[0-9]0", "dc=dc1", "disk!=disk1", "metric=disk_time"}) {
		queries = append(queries, testQuery{Expressions: expressions, ExpectedResults: 90})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		q := queries[n%len(queries)]
		query, err := tagquery.NewQueryFromStrings(q.Expressions, 150000)
		if err != nil {
			b.Fatalf(err.Error())
		}
		series := ix.FindByTag(1, query)
		if len(series) != q.ExpectedResults {
			b.Fatalf("%+v expected %d got %d results instead", q.Expressions, q.ExpectedResults, len(series))
		}
	}
}

// since that's going through a lot of permutations it needs an increased
// benchtime to be meaningful. f.e. on my laptop i'm using -benchtime=1m, which
// is enough for it to go through all the 5! permutations
func BenchmarkTagQueryFilterAndIntersectOnlyRegex(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagQueryFilterAndIntersectOnlyRegex)(b)
}

func benchmarkTagQueryFilterAndIntersectOnlyRegex(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	queries := make([]testQuery, 0)
	for _, expressions := range permutations([]string{"metric!=~.*_time$", "dc=~.*0$", "direction=~wri", "host=~host9[0-9]0", "disk!=~disk[5-9]{1}"}) {
		queries = append(queries, testQuery{Expressions: expressions, ExpectedResults: 150})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		q := queries[n%len(queries)]
		query, err := tagquery.NewQueryFromStrings(q.Expressions, 0)
		if err != nil {
			b.Fatalf(err.Error())
		}
		series := ix.FindByTag(1, query)
		if len(series) != q.ExpectedResults {
			b.Fatalf("%+v expected %d got %d results instead", q.Expressions, q.ExpectedResults, len(series))
		}
	}
}

func BenchmarkTagQueryKeysByPrefixSimple(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagQueryKeysByPrefixSimple)(b)
}

func benchmarkTagQueryKeysByPrefixSimple(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	type testCase struct {
		prefix string
		expRes []string
	}

	tc := testCase{
		prefix: "di",
		expRes: []string{"direction", "disk"},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		autoCompleteTagsAndCompare(b, n, tc.prefix, 2, tc.expRes)
	}
}

func BenchmarkTagQueryKeysByPrefixExpressions(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagQueryKeysByPrefixExpressions)(b)
}

func benchmarkTagQueryKeysByPrefixExpressions(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	type testCase struct {
		prefix string
		expr   []string
		expRes []string
	}

	tc := testCase{
		prefix: "di",
		expr:   []string{"metric=~.*_time$", "direction!=~re", "host=~host9[0-9]0"},
		expRes: []string{"direction", "disk"},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		autoCompleteTagsWithQueryAndCompare(b, n, tc.prefix, tc.expr, 2, tc.expRes)
	}
}
