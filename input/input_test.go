package input

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/cache"
	backendStore "github.com/grafana/metrictank/store"
	"github.com/raintank/schema"
)

func BenchmarkProcessMetricDataUniqueMetrics(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := backendStore.NewDevnullStore()

	mdata.SetSingleSchema(conf.NewRetentionMT(10, 10000, 600, 10, true))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, false, 800, 8000, 0)
	metricIndex := memory.New()
	metricIndex.Init()
	in := NewDefaultHandler(aggmetrics, metricIndex, "BenchmarkProcess")

	// timestamps start at 10 and go up from there. (we can't use 0, see AggMetric.Add())
	datas := make([]*schema.MetricData, b.N)
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("fake.metric.%d", i)
		metric := &schema.MetricData{
			Id:       "some.id.of.a.metric",
			OrgId:    500,
			Name:     name,
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
		in.ProcessMetricData(datas[i], 1)
	}
}

func BenchmarkProcessMetricDataSameMetric(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := backendStore.NewDevnullStore()

	mdata.SetSingleSchema(conf.NewRetentionMT(10, 10000, 600, 10, true))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, false, 800, 8000, 0)
	metricIndex := memory.New()
	metricIndex.Init()
	in := NewDefaultHandler(aggmetrics, metricIndex, "BenchmarkProcess")

	// timestamps start at 10 and go up from there. (we can't use 0, see AggMetric.Add())
	datas := make([]*schema.MetricData, b.N)
	for i := 0; i < b.N; i++ {
		name := "fake.metric.same"
		metric := &schema.MetricData{
			Id:       "some.id.of.a.metric",
			OrgId:    500,
			Name:     name,
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
		in.ProcessMetricData(datas[i], 1)
	}
}
