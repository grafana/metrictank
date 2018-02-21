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
	"github.com/grafana/metrictank/mdata/memorystore"
	backendStore "github.com/grafana/metrictank/store"
	"gopkg.in/raintank/schema.v1"
)

func BenchmarkProcessUniqueMetrics(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := backendStore.NewDevnullStore()
	mdata.BackendStore = store
	mdata.Cache = &cache.MockCache{}
	mdata.SetSingleSchema(conf.NewRetentionMT(10, 10000, 600, 10, true))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := memorystore.NewAggMetrics(800, 8000, 0)
	mdata.MemoryStore = aggmetrics
	metricIndex := memory.New()
	mdata.Idx = metricIndex
	metricIndex.Init()
	in := NewDefaultHandler("BenchmarkProcess")

	// timestamps start at 10 and go up from there. (we can't use 0, see AggMetric.Add())
	datas := make([]*schema.MetricData, b.N)
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("fake.metric.%d", i)
		metric := &schema.MetricData{
			Id:       "some.id.of.a.metric",
			OrgId:    500,
			Name:     name,
			Metric:   name,
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

func BenchmarkProcessSameMetric(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := backendStore.NewDevnullStore()
	mdata.BackendStore = store
	mdata.Cache = &cache.MockCache{}
	mdata.SetSingleSchema(conf.NewRetentionMT(10, 10000, 600, 10, true))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := memorystore.NewAggMetrics(800, 8000, 0)
	mdata.MemoryStore = aggmetrics
	metricIndex := memory.New()
	mdata.Idx = metricIndex
	metricIndex.Init()
	in := NewDefaultHandler("BenchmarkProcess")

	// timestamps start at 10 and go up from there. (we can't use 0, see AggMetric.Add())
	datas := make([]*schema.MetricData, b.N)
	for i := 0; i < b.N; i++ {
		name := "fake.metric.same"
		metric := &schema.MetricData{
			Id:       "some.id.of.a.metric",
			OrgId:    500,
			Name:     name,
			Metric:   name,
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
