package memory

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/grafana/metrictank/idx"
	"gopkg.in/raintank/schema.v1"
)

var (
	ix         idx.MetricIndex
	queries    []query
	tagQueries []tagQuery
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
	_tagSupport := tagSupport
	defer func() { tagSupport = _tagSupport }()
	tagSupport = true

	Init()
	os.Exit(m.Run())
}

func Init() {
	ix = New()
	ix.Init()

	var data *schema.MetricData

	for _, series := range cpuMetrics(5, 1000, 0, 32, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	for _, series := range diskMetrics(5, 1000, 0, 10, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    1,
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	// orgId has 1,680,000 series

	for _, series := range cpuMetrics(5, 100, 950, 32, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    2,
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	for _, series := range diskMetrics(5, 100, 950, 10, "collectd") {
		data = &schema.MetricData{
			Name:     series.Name,
			Metric:   series.Name,
			Tags:     series.Tags,
			Interval: 10,
			OrgId:    2,
		}
		data.SetId()
		ix.AddOrUpdate(data, 1)
	}
	//orgId 2 has 168,000 mertics

	queries = []query{
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

	tagQueries = []tagQuery{
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
		{Expressions: []string{"dc=dc1", "host=host666", "cpu!=~cpu[0-9]{2}", "device!=~d.*"}, ExpectedResults: 16},
		{Expressions: []string{"dc=dc1", "host!=~host10[0-9]{2}", "device!=~c.*"}, ExpectedResults: 1500},
	}
}

func ixFind(org, q int) {
	nodes, err := ix.Find(org, queries[q].Pattern, 0)
	if err != nil {
		panic(err)
	}
	if len(nodes) != queries[q].ExpectedResults {
		for _, n := range nodes {
			fmt.Println(n.Path)
		}
		panic(fmt.Sprintf("%s expected %d got %d results instead", queries[q].Pattern, queries[q].ExpectedResults, len(nodes)))
	}
}

func BenchmarkFind(b *testing.B) {
	if ix == nil {
		Init()
	}
	queryCount := len(queries)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		org := (n % 2) + 1
		ixFind(org, q)
	}
}

type testQ struct {
	q   int
	org int
}

func BenchmarkConcurrent4Find(b *testing.B) {
	if ix == nil {
		Init()
	}

	queryCount := len(queries)
	if ix == nil {
		Init()
	}

	ch := make(chan testQ)
	for i := 0; i < 4; i++ {
		go func() {
			for q := range ch {
				ixFind(q.org, q.q)
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
	if ix == nil {
		Init()
	}

	queryCount := len(queries)
	if ix == nil {
		Init()
	}

	ch := make(chan testQ)
	for i := 0; i < 8; i++ {
		go func() {
			for q := range ch {
				ixFind(q.org, q.q)
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

func ixFindByTag(org, q int) {
	series, err := ix.IdsByTagExpressions(org, tagQueries[q].Expressions)
	if err != nil {
		panic(err)
	}
	if len(series) != tagQueries[q].ExpectedResults {
		for _, s := range series {
			fmt.Println(s)
		}
		panic(fmt.Sprintf("%+v expected %d got %d results instead", tagQueries[q].Expressions, tagQueries[q].ExpectedResults, len(series)))
	}
}

func BenchmarkTagFindSimpleIntersect(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % 2
		org := (n % 2) + 1
		ixFindByTag(org, q)
	}
}

func BenchmarkTagFindRegexIntersect(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := (n % 2) + 2
		org := (n % 2) + 1
		ixFindByTag(org, q)
	}
}

func BenchmarkConcurrent8TagFind(b *testing.B) {
	var wg sync.WaitGroup
	ch := make(chan testQ)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			for q := range ch {
				ixFindByTag(q.org, q.q)
			}
			wg.Done()
		}()
	}

	queryCount := len(tagQueries)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		org := (n % 2) + 1
		ch <- testQ{q: q, org: org}
	}
	close(ch)
	wg.Wait()
}
