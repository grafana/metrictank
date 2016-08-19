package idx

import (
	"fmt"
	"strconv"
	"testing"

	//"github.com/raintank/met/helper"
	"gopkg.in/raintank/schema.v1"
)

var (
	ix      *Idx
	queries []query
)

type query struct {
	Pattern         string
	ExpectedResults int
}

func cpuMetrics(dcCount, hostCount, hostOffset, cpuCount int, prefix string) []string {
	series := make([]string, 0)
	for dc := 0; dc < dcCount; dc++ {
		for host := hostOffset; host < hostCount+hostOffset; host++ {
			for cpu := 0; cpu < cpuCount; cpu++ {
				p := prefix + ".dc" + strconv.Itoa(dc) + ".host" + strconv.Itoa(host) + ".cpu." + strconv.Itoa(cpu)
				for _, metric := range []string{"idle", "interupt", "nice", "softirq", "steal", "system", "user", "wait"} {
					series = append(series, p+"."+metric)
				}
			}
		}
	}
	return series
}

func diskMetrics(dcCount, hostCount, hostOffset, diskCount int, prefix string) []string {
	series := make([]string, 0)
	for dc := 0; dc < dcCount; dc++ {
		for host := hostOffset; host < hostCount+hostOffset; host++ {
			for disk := 0; disk < diskCount; disk++ {
				p := prefix + ".dc" + strconv.Itoa(dc) + ".host" + strconv.Itoa(host) + ".disk.disk" + strconv.Itoa(disk)
				for _, metric := range []string{"disk_merged", "disk_octets", "disk_ops", "disk_time"} {
					series = append(series, p+"."+metric+".read", p+"."+metric+".write")
				}
			}
		}
	}
	return series
}

func init() {
	ix = New()
	//stats, _ := helper.New(false, "", "standard", "metrictank", "")
	//ix.Init(stats)

	var data schema.MetricDefinition

	for _, series := range cpuMetrics(5, 1000, 0, 32, "collectd") {
		data = schema.MetricDefinition{
			Name:     series,
			Metric:   series,
			Interval: 10,
			OrgId:    1,
		}
		data.SetId()
		ix.Add(data)
	}
	for _, series := range diskMetrics(5, 1000, 0, 10, "collectd") {
		data = schema.MetricDefinition{
			Name:     series,
			Metric:   series,
			Interval: 10,
			OrgId:    1,
		}
		data.SetId()
		ix.Add(data)
	}
	// orgId has 1,680,000 series

	for _, series := range cpuMetrics(5, 100, 950, 32, "collectd") {
		data = schema.MetricDefinition{
			Name:     series,
			Metric:   series,
			Interval: 10,
			OrgId:    2,
		}
		data.SetId()
		ix.Add(data)
	}
	for _, series := range diskMetrics(5, 100, 950, 10, "collectd") {
		data = schema.MetricDefinition{
			Name:     series,
			Metric:   series,
			Interval: 10,
			OrgId:    2,
		}
		data.SetId()
		ix.Add(data)
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
	}
}

func BenchmarkFind(b *testing.B) {
	queryCount := len(queries)
	org := 1
	failCount := 0
	for n := 0; n < b.N; n++ {
		q := n % queryCount
		_, nodes, _ := ix.Match(org, queries[q].Pattern)
		if len(nodes) != queries[q].ExpectedResults {
			failCount++
		}
		if org == 1 {
			org = 2
		} else {
			org = 1
		}
	}
	b.Log(fmt.Sprintf("%d searches failed. ", failCount))
}
