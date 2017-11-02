package memory

import (
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/idx"
	"gopkg.in/raintank/schema.v1"
)

var (
	ix           idx.MetricIndex
	currentIndex int // 1 small; 2 large
)

type query struct {
	Pattern         string
	ExpectedResults int
}

type tagQuery struct {
	Expressions     []string
	ExpectedResults int
}

type metric struct {
	Name string
	Tags []string
}

var queries = []query{
	//LEAF queries
	{Pattern: "collectd.dc1.host960.disk.disk1.disk_ops.read", ExpectedResults: 1},
	{Pattern: "collectd.dc1.host960.disk.disk1.disk_ops.*", ExpectedResults: 2},
	{Pattern: "collectd.*.host960.disk.disk1.disk_ops.read", ExpectedResults: 5},
	{Pattern: "collectd.*.host960.disk.disk1.disk_ops.*", ExpectedResults: 10},
	{Pattern: "collectd.d*.host960.disk.disk1.disk_ops.*", ExpectedResults: 10},
	{Pattern: "collectd.[abcd]*.host960.disk.disk1.disk_ops.*", ExpectedResults: 10},
	{Pattern: "collectd.{dc1,dc50}.host960.disk.disk1.disk_ops.*", ExpectedResults: 2},

	{Pattern: "collectd.dc3.host960.cpu.1.idle", ExpectedResults: 1},
	{Pattern: "collectd.dc30.host960.cpu.1.idle", ExpectedResults: 0},
	{Pattern: "collectd.dc3.host960.*.*.idle", ExpectedResults: 32},
	{Pattern: "collectd.dc3.host960.*.*.idle", ExpectedResults: 32},

	{Pattern: "collectd.dc3.host96[0-9].cpu.1.idle", ExpectedResults: 10},
	{Pattern: "collectd.dc30.host96[0-9].cpu.1.idle", ExpectedResults: 0},
	{Pattern: "collectd.dc3.host96[0-9].*.*.idle", ExpectedResults: 320},
	{Pattern: "collectd.dc3.host96[0-9].*.*.idle", ExpectedResults: 320},

	{Pattern: "collectd.{dc1,dc2,dc3}.host960.cpu.1.idle", ExpectedResults: 3},
	{Pattern: "collectd.{dc*, a*}.host960.cpu.1.idle", ExpectedResults: 5},

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

var tagQueries = []tagQuery{
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
							"dc=dc" + strconv.Itoa(dc),
							"host=host" + strconv.Itoa(host),
							"device=cpu",
							"cpu=cpu" + strconv.Itoa(cpu),
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
							"host=host" + strconv.Itoa(host),
							"device=disk",
							"disk=disk" + strconv.Itoa(disk),
							"metric=" + m,
							"direction=read",
						},
					})
					series = append(series, metric{
						Name: p + "." + m + ".write",
						Tags: []string{
							"dc=dc" + strconv.Itoa(dc),
							"host=host" + strconv.Itoa(host),
							"device=disk",
							"disk=disk" + strconv.Itoa(disk),
							"metric=" + m,
							"direction=write",
						},
					})
				}
			}
		}
	}
	return series
}

func TestMain(m *testing.M) {
	defer func(t bool) { tagSupport = t }(tagSupport)
	tagSupport = true
	matchCacheSize = 1000
	os.Exit(m.Run())
}

func InitSmallIndex() {
	// if the current index is not the small index then initialize it
	if currentIndex != 1 {
		ix = nil

		// run GC because we only get 4G on CircleCI
		runtime.GC()

		ix = New()
		ix.Init()

		currentIndex = 1
	} else {
		return
	}

	var data *schema.MetricData

	for i, series := range cpuMetrics(5, 100, 0, 32, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
			Time:     int64(i + 100),
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	for i, series := range diskMetrics(5, 100, 0, 10, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
			Time:     int64(i + 100),
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
}

func InitLargeIndex() {
	// if the current index is not the large index then initialize it
	if currentIndex != 2 {
		ix = nil

		// run GC because we only get 4G on CircleCI
		runtime.GC()

		ix = New()
		ix.Init()

		currentIndex = 2
	} else {
		return
	}

	var data *schema.MetricData

	for i, series := range cpuMetrics(5, 1000, 0, 32, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
			Time:     int64(i + 100),
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	for i, series := range diskMetrics(5, 1000, 0, 10, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
			Time:     int64(i + 100),
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	// orgId 1 has 1,680,000 series

	for i, series := range cpuMetrics(5, 100, 950, 32, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    2,
			Time:     int64(i + 100),
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	for i, series := range diskMetrics(5, 100, 950, 10, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    2,
			Time:     int64(i + 100),
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	//orgId 2 has 168,000 mertics
}

func queryAndCompareTagValues(t *testing.T, key, filter string, from int64, expected map[string]uint64) {
	t.Helper()

	values, err := ix.TagDetails(1, key, filter, from)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
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
	InitSmallIndex()

	expected := make(map[string]uint64)
	expected["dc0"] = 33600
	expected["dc1"] = 33600
	expected["dc2"] = 33600
	expected["dc3"] = 33600
	expected["dc4"] = 33600
	queryAndCompareTagValues(t, "dc", "", 0, expected)
}

func TestTagDetailsWithFrom(t *testing.T) {
	InitSmallIndex()

	expected := make(map[string]uint64)
	expected["dc3"] = 2500
	expected["dc4"] = 25600

	queryAndCompareTagValues(t, "dc", "", 100000, expected)
}

func TestTagDetailsWithFilter(t *testing.T) {
	InitSmallIndex()

	expected := make(map[string]uint64)
	expected["dc3"] = 33600
	expected["dc4"] = 33600

	queryAndCompareTagValues(t, "dc", ".+[3-9]{1}$", 0, expected)
}

func TestTagDetailsWithFilterAndFrom(t *testing.T) {
	InitSmallIndex()

	expected := make(map[string]uint64)
	expected["dc4"] = 25600

	queryAndCompareTagValues(t, "dc", ".+[4-9]{1}$", 100000, expected)
}

func queryAndCompareTagKeys(t testing.TB, filter string, from int64, expected []string) {
	t.Helper()
	values, err := ix.Tags(1, filter, from)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	if len(values) != len(expected) {
		t.Fatalf("Expected %d values, but got %d", len(expected), len(values))
	}

	sort.Strings(expected)
	sort.Strings(values)

	if !reflect.DeepEqual(values, expected) {
		t.Fatalf("Expected values:\n%+v\nGot:\n%+v", expected, values)
	}

}

func TestTagKeysWithoutFilters(t *testing.T) {
	InitSmallIndex()

	expected := []string{"dc", "host", "device", "cpu", "metric", "direction", "disk"}
	queryAndCompareTagKeys(t, "", 0, expected)
}

func TestTagKeysWithFrom(t *testing.T) {
	InitSmallIndex()

	// disk metrics should all have been added before ts 1000000
	expected := []string{"dc", "host", "device", "cpu", "metric"}
	queryAndCompareTagKeys(t, "", 100000, expected)
}

func TestTagKeysWithFilter(t *testing.T) {
	InitSmallIndex()

	expected := []string{"dc", "device", "disk", "direction"}
	queryAndCompareTagKeys(t, "d", 0, expected)

	expected = []string{"disk", "direction"}
	queryAndCompareTagKeys(t, "di", 0, expected)
}

func TestTagKeysWithFromAndFilter(t *testing.T) {
	InitSmallIndex()

	expected := []string{"dc", "device"}
	queryAndCompareTagKeys(t, "d", 100000, expected)

	// reflect.DeepEqual treats nil & []string{} as not equal
	queryAndCompareTagKeys(t, "di", 1000000, nil)
}

func TestTagSorting(t *testing.T) {
	index := New()
	index.Init()

	md1 := &schema.MetricData{
		Name:     "name1",
		Metric:   "name1",
		Tags:     []string{},
		Interval: 10,
		OrgId:    1,
		Time:     int64(123),
	}
	md1.SetId()

	// set out of order tags after SetId (because that would sort it)
	md1.Tags = []string{"d=a", "b=a", "c=a", "a=a", "e=a"}
	index.AddOrUpdate(md1, 1)

	res, err := index.FindByTag(1, []string{"b=a"}, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if len(res) != 1 {
		t.Fatalf("Expected exactly 1 result, got %d: %+v", len(res), res)
	}
	expected := "name1;a=a;b=a;c=a;d=a;e=a"
	if res[0] != expected {
		t.Fatalf("Wrong metric name returned.\nExpected: %s\nGot: %s\n", expected, res[0])
	}

	md2 := []schema.MetricDefinition{
		{
			Name:       "name2",
			Metric:     "name2",
			Tags:       []string{},
			Interval:   10,
			OrgId:      1,
			LastUpdate: int64(123),
		},
	}
	md2[0].SetId()

	// set out of order tags after SetId (because that would sort it)
	md2[0].Tags = []string{"5=a", "1=a", "2=a", "4=a", "3=a"}
	index.Load(md2)

	res, err = index.FindByTag(1, []string{"3=a"}, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if len(res) != 1 {
		t.Fatalf("Expected exactly 1 result, got %d: %+v", len(res), res)
	}
	expected = "name2;1=a;2=a;3=a;4=a;5=a"
	if res[0] != expected {
		t.Fatalf("Wrong metric name returned.\nExpected: %s\nGot: %s\n", expected, res[0])
	}
}

func BenchmarkTagDetailsWithoutFromNorFilter(b *testing.B) {
	InitLargeIndex()

	b.ReportAllocs()
	b.ResetTimer()

	expectedCount := uint64(100000)
	for n := 0; n < b.N; n++ {
		val, _ := ix.TagDetails(1, "metric", "", int64(0))
		if val["disk_ops"] != expectedCount {
			b.Fatalf("Expected count %d, but got %d: %+v", expectedCount, val["disk_ops"], val)
		}
	}
}

func BenchmarkTagDetailsWithFromAndFilter(b *testing.B) {
	InitLargeIndex()

	filters := []string{"i", ".+rr", ".+t", "interrupt"}
	expectedCounts := []uint64{158762, 158750, 158737, 158725}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		q := n % len(filters)
		val, _ := ix.TagDetails(1, "metric", filters[q], int64(10000+(q*100)))
		if val["interrupt"] != expectedCounts[q] {
			b.Fatalf("Expected count %d, but got %d: %+v", expectedCounts[q], val["interrupt"], val)
		}
	}
}

func BenchmarkTagsWithFromAndFilter(b *testing.B) {
	InitLargeIndex()
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
	InitLargeIndex()
	expected := []string{"dc", "device", "direction", "disk", "cpu", "metric", "host"}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		queryAndCompareTagKeys(b, "", 0, expected)
	}
}

func ixFind(b *testing.B, org, q int) {
	nodes, err := ix.Find(org, queries[q].Pattern, 0)
	if err != nil {
		panic(err)
	}
	if len(nodes) != queries[q].ExpectedResults {
		for _, n := range nodes {
			b.Log(n.Path)
		}
		b.Fatalf("%s expected %d got %d results instead", queries[q].Pattern, queries[q].ExpectedResults, len(nodes))
	}
}

func BenchmarkFind(b *testing.B) {
	InitLargeIndex()
	queryCount := len(queries)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		org := (n % 2) + 1
		ixFind(b, org, q)
	}
}

type testQ struct {
	q   int
	org int
}

func BenchmarkConcurrent4Find(b *testing.B) {
	InitLargeIndex()
	queryCount := len(queries)

	ch := make(chan testQ)
	for i := 0; i < 4; i++ {
		go func() {
			for q := range ch {
				ixFind(b, q.org, q.q)
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		org := (n % 2) + 1
		ch <- testQ{q: q, org: org}
	}
	close(ch)
}

func BenchmarkConcurrent8Find(b *testing.B) {
	InitLargeIndex()
	queryCount := len(queries)

	ch := make(chan testQ)
	for i := 0; i < 8; i++ {
		go func() {
			for q := range ch {
				ixFind(b, q.org, q.q)
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		org := (n % 2) + 1
		ch <- testQ{q: q, org: org}
	}
	close(ch)
}

func ixFindByTag(b *testing.B, org, q int) {
	series, err := ix.FindByTag(org, tagQueries[q].Expressions, 0)
	if err != nil {
		panic(err)
	}
	if len(series) != tagQueries[q].ExpectedResults {
		for _, s := range series {
			memoryIdx := ix.(*MemoryIdx)
			b.Log(memoryIdx.DefById[s].Tags)
		}
		b.Fatalf("%+v expected %d got %d results instead", tagQueries[q].Expressions, tagQueries[q].ExpectedResults, len(series))
	}
}

func BenchmarkTagFindSimpleIntersect(b *testing.B) {
	InitLargeIndex()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % 2
		org := (n % 2) + 1
		ixFindByTag(b, org, q)
	}
}

func BenchmarkTagFindRegexIntersect(b *testing.B) {
	InitLargeIndex()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := (n % 2) + 2
		org := (n % 2) + 1
		ixFindByTag(b, org, q)
	}
}

func BenchmarkTagFindMatchingAndFiltering(b *testing.B) {
	InitLargeIndex()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := (n % 2) + 4
		org := (n % 2) + 1
		ixFindByTag(b, org, q)
	}
}

func BenchmarkTagFindMatchingAndFilteringWithRegex(b *testing.B) {
	InitLargeIndex()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := (n % 2) + 6
		org := (n % 2) + 1
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
	InitLargeIndex()

	queries := make([]tagQuery, 0)
	for _, expressions := range permutations([]string{"direction!=~read", "device!=", "host=~host9[0-9]0", "dc=dc1", "disk!=disk1", "metric=disk_time"}) {
		queries = append(queries, tagQuery{Expressions: expressions, ExpectedResults: 90})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		q := queries[n%len(queries)]
		series, err := ix.FindByTag(1, q.Expressions, 150000)
		if err != nil {
			b.Fatalf(err.Error())
		}
		if len(series) != q.ExpectedResults {
			b.Fatalf("%+v expected %d got %d results instead", q.Expressions, q.ExpectedResults, len(series))
		}
	}
}

// since that's going through a lot of permutations it needs an increased
// benchtime to be meaningful. f.e. on my laptop i'm using -benchtime=1m, which
// is enough for it to go through all the 5! permutations
func BenchmarkTagQueryFilterAndIntersectOnlyRegex(b *testing.B) {
	InitLargeIndex()

	queries := make([]tagQuery, 0)
	for _, expressions := range permutations([]string{"metric!=~.*_time$", "dc=~.*0$", "direction=~wri", "host=~host9[0-9]0", "disk!=~disk[5-9]{1}"}) {
		queries = append(queries, tagQuery{Expressions: expressions, ExpectedResults: 150})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		q := queries[n%len(queries)]
		series, err := ix.FindByTag(1, q.Expressions, 0)
		if err != nil {
			b.Fatalf(err.Error())
		}
		if len(series) != q.ExpectedResults {
			b.Fatalf("%+v expected %d got %d results instead", q.Expressions, q.ExpectedResults, len(series))
		}
	}
}
