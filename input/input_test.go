package input

import (
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/cache"
	"github.com/raintank/metrictank/usage"
	"gopkg.in/raintank/schema.v1"
)

func Test_Process(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	store := mdata.NewDevnullStore()
	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, 600, 10, 800, 8000, 10000, 0, make([]mdata.AggSetting, 0))
	metricIndex := memory.New()
	metricIndex.Init()
	usage := usage.New(300, aggmetrics, metricIndex, clock.New())
	in := NewDefaultHandler(aggmetrics, metricIndex, usage, "TestProcess")

	allMetrics := make(map[string]int)
	for i := 0; i < 5; i++ {
		metrics := test_Process(i, &in, t)
		for mId, id := range metrics {
			allMetrics[mId] = id
		}
	}
	defs := metricIndex.List(-1)
	if len(defs) != 13 {
		t.Fatalf("query for org -1 should result in 13 distinct metrics. not %d", len(defs))
	}

	for _, d := range defs {
		id := allMetrics[d.Id]
		if d.Name != fmt.Sprintf("some.id.%d", id) {
			t.Fatalf("incorrect name for %s : %s", d.Id, d.Name)
		}
		if d.OrgId != id {
			t.Fatalf("incorrect OrgId for %s : %d", d.Id, d.OrgId)
		}
		if d.Tags[0] != fmt.Sprintf("%d", id) {
			t.Fatalf("incorrect tags for %s : %s", d.Id, d.Tags)
		}
	}

	defs = metricIndex.List(2)
	if len(defs) != 1 {
		t.Fatalf("len of defs should be exactly 1. got defs with len %d: %v", len(defs), defs)
	}
	d := defs[0]
	if d.OrgId != 2 {
		t.Fatalf("incorrect metricdef returned: %v", d)
	}
}

func test_Process(worker int, in Handler, t *testing.T) map[string]int {
	var metric *schema.MetricData
	metrics := make(map[string]int)
	for m := 0; m < 4; m++ {
		id := (worker + 1) * (m + 1)
		t.Logf("worker %d metric %d -> adding metric with id and orgid %d", worker, m, id)

		metric = &schema.MetricData{
			Id:       "",
			OrgId:    id,
			Name:     fmt.Sprintf("some.id.%d", id),
			Metric:   fmt.Sprintf("some.id.%d", id),
			Interval: 60,
			Value:    1234.567,
			Unit:     "ms",
			Time:     int64(id),
			Mtype:    "gauge",
			Tags:     []string{fmt.Sprintf("%d", id)},
		}
		metric.SetId()
		metrics[metric.Id] = id
		in.Process(metric, 1)
	}
	return metrics
}

func BenchmarkProcess(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := mdata.NewDevnullStore()
	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, 600, 10, 800, 8000, 10000, 0, make([]mdata.AggSetting, 0))
	metricIndex := memory.New()
	metricIndex.Init()
	usage := usage.New(300, aggmetrics, metricIndex, clock.New())
	in := NewDefaultHandler(aggmetrics, metricIndex, usage, "BenchmarkProcess")

	// timestamps start at 10 and go up from there. (we can't use 0, see AggMetric.Add())
	datas := make([]*schema.MetricData, b.N)
	for i := 0; i < b.N; i++ {
		metric := &schema.MetricData{
			Id:       "some.id.of.a.metric",
			OrgId:    500,
			Name:     "some.id",
			Metric:   "metric",
			Interval: 10,
			Value:    1234.567,
			Unit:     "ms",
			Time:     int64((i + 1) * 10),
			Mtype:    "gauge",
			Tags:     []string{"some_tag", "ok"},
		}
		datas[i] = metric
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in.Process(datas[i], 1)
	}
}
