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
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

var (
	ix                   MemoryIndex
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

func queryAndCompareTagValues(t *testing.T, key, filter string, from int64, expected map[string]uint64) {
	t.Helper()

	values := ix.TagDetails(1, key, regexp.MustCompile(filter), from)
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

func TestTagDetailsWithoutFilters(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagDetailsWithoutFilters)(t)
}

func testTagDetailsWithoutFilters(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	expected := make(map[string]uint64)
	expected["dc0"] = 33600
	expected["dc1"] = 33600
	expected["dc2"] = 33600
	expected["dc3"] = 33600
	expected["dc4"] = 33600
	queryAndCompareTagValues(t, "dc", "", 0, expected)
}

func TestTagDetailsWithFrom(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagDetailsWithFrom)(t)
}

func testTagDetailsWithFrom(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	expected := make(map[string]uint64)
	expected["dc3"] = 2500
	expected["dc4"] = 25600

	queryAndCompareTagValues(t, "dc", "", 100000, expected)
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

	queryAndCompareTagValues(t, "dc", ".+[3-9]{1}$", 0, expected)
}

func TestTagDetailsWithFilterAndFrom(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagDetailsWithFilterAndFrom)(t)
}

func testTagDetailsWithFilterAndFrom(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	expected := make(map[string]uint64)
	expected["dc4"] = 25600

	queryAndCompareTagValues(t, "dc", ".+[4-9]{1}$", 100000, expected)
}

func queryAndCompareTagKeys(t testing.TB, filter string, from int64, expected []string) {
	t.Helper()
	values := ix.Tags(1, regexp.MustCompile(filter), from)
	if len(values) != len(expected) {
		t.Fatalf("Expected %d values, but got %d", len(expected), len(values))
	}

	sort.Strings(expected)
	sort.Strings(values)

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
	queryAndCompareTagKeys(t, "", 0, expected)
}

func TestTagKeysWithFrom(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagKeysWithFrom)(t)
}

func testTagKeysWithFrom(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	// disk metrics should all have been added before ts 1000000
	expected := []string{"dc", "host", "device", "cpu", "metric", "name"}
	queryAndCompareTagKeys(t, "", 100000, expected)
}

func TestTagKeysWithFilter(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagKeysWithFilter)(t)
}

func testTagKeysWithFilter(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	expected := []string{"dc", "device", "disk", "direction"}
	queryAndCompareTagKeys(t, "d", 0, expected)

	expected = []string{"direction", "disk"}
	queryAndCompareTagKeys(t, "di", 0, expected)
}

func TestTagKeysWithFromAndFilter(t *testing.T) {
	withAndWithoutPartitonedIndex(testTagKeysWithFromAndFilter)(t)
}

func testTagKeysWithFromAndFilter(t *testing.T) {
	InitSmallIndex()
	defer ix.Stop()

	expected := []string{"dc", "device"}
	queryAndCompareTagKeys(t, "d", 100000, expected)

	queryAndCompareTagKeys(t, "di", 1000000, nil)
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

	md2 := []idx.MetricDefinition{
		{
			Tags:       idx.TagKeyValues{},
			Interval:   10,
			OrgId:      1,
			LastUpdate: int64(123),
		},
	}
	md2[0].SetMetricName("name2")
	md2[0].SetId()

	// set out of order tags after SetId (because that would sort it)
	// e.g. mimic the case where somebody sent us a MD with an id already set and out-of-order tags
	var k, v uintptr
	md2[0].Tags = idx.TagKeyValues{}
	k, _ = idx.IdxIntern.AddOrGet([]byte("5"), false)
	v, _ = idx.IdxIntern.AddOrGet([]byte("a"), false)
	md2[0].Tags.KeyValues = append(md2[0].Tags.KeyValues, idx.TagKeyValue{Key: k, Value: v})
	k, _ = idx.IdxIntern.AddOrGet([]byte("1"), false)
	v, _ = idx.IdxIntern.AddOrGet([]byte("a"), false)
	md2[0].Tags.KeyValues = append(md2[0].Tags.KeyValues, idx.TagKeyValue{Key: k, Value: v})
	k, _ = idx.IdxIntern.AddOrGet([]byte("2"), false)
	v, _ = idx.IdxIntern.AddOrGet([]byte("a"), false)
	md2[0].Tags.KeyValues = append(md2[0].Tags.KeyValues, idx.TagKeyValue{Key: k, Value: v})
	k, _ = idx.IdxIntern.AddOrGet([]byte("4"), false)
	v, _ = idx.IdxIntern.AddOrGet([]byte("a"), false)
	md2[0].Tags.KeyValues = append(md2[0].Tags.KeyValues, idx.TagKeyValue{Key: k, Value: v})
	k, _ = idx.IdxIntern.AddOrGet([]byte("3"), false)
	v, _ = idx.IdxIntern.AddOrGet([]byte("a"), false)
	md2[0].Tags.KeyValues = append(md2[0].Tags.KeyValues, idx.TagKeyValue{Key: k, Value: v})
	index.Load(md2)

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

	type testCase struct {
		prefix string
		from   int64
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			prefix: "ho",
			from:   100,
			limit:  100,
			expRes: []string{"host"},
		}, {
			prefix: "host",
			from:   100,
			limit:  100,
			expRes: []string{"host"},
		}, {
			prefix: "n",
			from:   100,
			limit:  100,
			expRes: []string{"name"},
		}, {
			prefix: "",
			from:   100,
			limit:  100,
			expRes: []string{"cpu", "dc", "device", "direction", "disk", "host", "metric", "name"},
		},
	}

	for i, tc := range testCases {
		autoCompleteTagsAndCompare(t, i, tc.prefix, tc.from, tc.limit, tc.expRes)
	}
}

func autoCompleteTagsAndCompare(t testing.TB, tcIdx int, prefix string, from int64, limit uint, expRes []string) {
	t.Helper()

	res := ix.FindTags(1, prefix, from, limit)

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
		from   int64
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			prefix: "di",
			expr:   []string{"direction=write", "host=host90"},
			from:   100,
			limit:  100,
			expRes: []string{"direction", "disk"},
		}, {
			prefix: "di",
			expr:   []string{"direction=write", "host=host90", "device=cpu"},
			from:   100,
			limit:  100,
			expRes: []string{},
		}, {
			prefix: "",
			expr:   []string{"direction=write", "host=host90"},
			from:   100,
			limit:  100,
			expRes: []string{"dc", "device", "direction", "disk", "host", "metric", "name"},
		}, {
			prefix: "",
			expr:   []string{"direction=write", "host=host90"},
			from:   100,
			limit:  3,
			expRes: []string{"dc", "device", "direction"},
		},
	}

	for i, tc := range testCases {
		autoCompleteTagsWithQueryAndCompare(t, i, tc.prefix, tc.expr, tc.from, tc.limit, tc.expRes)
	}
}

func autoCompleteTagsWithQueryAndCompare(t testing.TB, tcIdx int, prefix string, expr []string, from int64, limit uint, expRes []string) {
	t.Helper()

	query, err := tagquery.NewQueryFromStrings(expr, from)
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

	type testCase struct {
		tag    string
		prefix string
		from   int64
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			tag:    "device",
			prefix: "",
			from:   100,
			limit:  100,
			expRes: []string{"cpu", "disk"},
		},
	}

	for _, tc := range testCases {
		autoCompleteTagValuesAndCompare(t, tc.tag, tc.prefix, tc.from, tc.limit, tc.expRes)
	}
}

func autoCompleteTagValuesAndCompare(t testing.TB, tag, prefix string, from int64, limit uint, expRes []string) {
	t.Helper()

	res := ix.FindTagValues(1, tag, prefix, from, limit)

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
		from   int64
		limit  uint
		expRes []string
	}

	testCases := []testCase{
		{
			tag:    "host",
			prefix: "host9",
			expr:   []string{"direction=write"},
			from:   100,
			limit:  100,
			expRes: []string{"host9", "host90", "host91", "host92", "host93", "host94", "host95", "host96", "host97", "host98", "host99"},
		}, {
			tag:    "host",
			prefix: "host9",
			expr:   []string{"direction=write"},
			from:   100,
			limit:  5,
			expRes: []string{"host9", "host90", "host91", "host92", "host93"},
		}, {
			tag:    "direction",
			prefix: "w",
			expr:   []string{"device=disk"},
			from:   100,
			limit:  100,
			expRes: []string{"write"},
		}, {
			tag:    "direction",
			prefix: "w",
			expr:   []string{"device=cpu"},
			from:   100,
			limit:  100,
			expRes: []string{},
		}, {
			tag:    "device",
			prefix: "",
			expr:   []string{"disk=~disk[4-5]{1}"},
			from:   100,
			limit:  100,
			expRes: []string{"disk"},
		},
	}

	for _, tc := range testCases {
		autoCompleteTagValuesWithQueryAndCompare(t, tc.tag, tc.prefix, tc.expr, tc.from, tc.limit, tc.expRes)
	}
}

func autoCompleteTagValuesWithQueryAndCompare(t testing.TB, tag, prefix string, expr []string, from int64, limit uint, expRes []string) {
	t.Helper()

	query, err := tagquery.NewQueryFromStrings(expr, from)
	if err != nil {
		t.Fatalf("Unexpected error when instantiating query: %s", err)
	}
	res := ix.FindTagValuesWithQuery(1, tag, prefix, query, limit)

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

func BenchmarkTagDetailsWithoutFromNorFilter(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagDetailsWithoutFromNorFilter)(b)
}

func benchmarkTagDetailsWithoutFromNorFilter(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	b.ReportAllocs()
	b.ResetTimer()

	expectedCount := uint64(100000)
	for n := 0; n < b.N; n++ {
		val := ix.TagDetails(1, "metric", regexp.MustCompile(""), int64(0))
		if val["disk_ops"] != expectedCount {
			b.Fatalf("Expected count %d, but got %d: %+v", expectedCount, val["disk_ops"], val)
		}
	}
}

func BenchmarkTagDetailsWithFromAndFilter(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagDetailsWithFromAndFilter)(b)
}

func benchmarkTagDetailsWithFromAndFilter(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	filters := []string{"i", ".+rr", ".+t", "interrupt"}
	expectedCounts := []uint64{158762, 158750, 158737, 158725}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		q := n % len(filters)
		val := ix.TagDetails(1, "metric", regexp.MustCompile(filters[q]), int64(10000+(q*100)))
		if val["interrupt"] != expectedCounts[q] {
			b.Fatalf("Expected count %d, but got %d: %+v", expectedCounts[q], val["interrupt"], val)
		}
	}
}

func BenchmarkTagsWithFromAndFilter(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagsWithFromAndFilter)(b)
}

func benchmarkTagsWithFromAndFilter(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()
	filters := []string{"d", "di", "c"}
	expected := [][]string{
		{"dc", "device", "direction", "disk"},
		{"direction", "disk"},
		{"cpu"},
	}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		q := n % len(filters)
		from := int64(q % 1000)
		queryAndCompareTagKeys(b, filters[q], from, expected[q])
	}
}

func BenchmarkTagsWithoutFromNorFilter(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagsWithoutFromNorFilter)(b)
}

func benchmarkTagsWithoutFromNorFilter(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()
	expected := []string{"dc", "device", "direction", "disk", "cpu", "metric", "name", "host"}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		queryAndCompareTagKeys(b, "", 0, expected)
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
	benchWithAndWithoutPartitonedIndex(benchmarkTagQueryFilterAndIntersect)(b)
}

func benchmarkTagQueryFilterAndIntersect(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	type testCase struct {
		query           tagquery.Query
		expectedResults int
	}
	var testCases []testCase
	for _, expressions := range permutations([]string{"direction!=~read", "device!=", "host=~host9[0-9]0", "dc=dc1", "disk!=disk1", "metric=disk_time"}) {
		query, err := tagquery.NewQueryFromStrings(expressions, 150000)
		if err != nil {
			b.Fatalf("Failed to instantiate query: %s", err.Error())
		}
		testCases = append(testCases, testCase{query: query, expectedResults: 90})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		tc := testCases[n%len(testCases)]
		series := ix.FindByTag(1, tc.query)
		if len(series) != tc.expectedResults {
			b.Fatalf("%+v expected %d got %d results instead", tc.query, tc.expectedResults, len(series))
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
		from   int64
		expRes []string
	}

	tc := testCase{
		prefix: "di",
		from:   100,
		expRes: []string{"direction", "disk"},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		autoCompleteTagsAndCompare(b, n, tc.prefix, tc.from, 2, tc.expRes)
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
		from   int64
		expRes []string
	}

	tc := testCase{
		prefix: "di",
		expr:   []string{"metric=~.*_time$", "direction!=~re", "host=~host9[0-9]0"},
		from:   12345,
		expRes: []string{"direction", "disk"},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		autoCompleteTagsWithQueryAndCompare(b, n, tc.prefix, tc.expr, tc.from, 2, tc.expRes)
	}
}

func BenchmarkTagQueryFilterByEqualExpression(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagQueryFilterByEqualExpression)(b)
}

func benchmarkTagQueryFilterByEqualExpression(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	query, err := tagquery.NewQueryFromStrings([]string{"dc=dc1", "cpu=cpu12"}, 0)
	if err != nil {
		b.Fatalf("Unexpected error when instantiating query: %s", err.Error())
	}
	expectedResults := 8000

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		series := ix.FindByTag(1, query)
		if len(series) != expectedResults {
			b.Fatalf("%+v expected %d got %d results instead", query, expectedResults, len(series))
		}
	}
}

func BenchmarkTagQueryFilterByMatchExpression(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagQueryFilterByMatchExpression)(b)
}

func benchmarkTagQueryFilterByMatchExpression(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	query, err := tagquery.NewQueryFromStrings([]string{"dc=dc1", "cpu=~cpu[0-9]2"}, 0)
	if err != nil {
		b.Fatalf("Unexpected error when instantiating query: %s", err.Error())
	}
	expectedResults := 16000

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		series := ix.FindByTag(1, query)
		if len(series) != expectedResults {
			b.Fatalf("%+v expected %d got %d results instead", query, expectedResults, len(series))
		}
	}
}

func BenchmarkTagQueryFilterByHasTagExpression(b *testing.B) {
	benchWithAndWithoutPartitonedIndex(benchmarkTagQueryFilterByHasTagExpression)(b)
}

func benchmarkTagQueryFilterByHasTagExpression(b *testing.B) {
	InitLargeIndex()
	defer ix.Stop()

	query, err := tagquery.NewQueryFromStrings([]string{"dc=dc1", "disk!="}, 0)
	if err != nil {
		b.Fatalf("Unexpected error when instantiating query: %s", err.Error())
	}
	expectedResults := 80000

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		series := ix.FindByTag(1, query)
		if len(series) != expectedResults {
			b.Fatalf("%+v expected %d got %d results instead", query, expectedResults, len(series))
		}
	}
}
