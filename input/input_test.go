package input

import (
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/conf"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/cache"
	"github.com/raintank/metrictank/usage"
	"gopkg.in/raintank/schema.v1"
)

func BenchmarkProcessUniqueMetrics(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := mdata.NewDevnullStore()

	mdata.SetSingleSchema(conf.NewRetentionMT(10, 10000, 600, 10, true))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, 800, 8000, 0)
	metricIndex := memory.New()
	metricIndex.Init()
	usage := usage.New(300, aggmetrics, metricIndex, clock.New())
	in := NewDefaultHandler(aggmetrics, metricIndex, usage, "BenchmarkProcess")

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

	store := mdata.NewDevnullStore()

	mdata.SetSingleSchema(conf.NewRetentionMT(10, 10000, 600, 10, true))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, 800, 8000, 0)
	metricIndex := memory.New()
	metricIndex.Init()
	usage := usage.New(300, aggmetrics, metricIndex, clock.New())
	in := NewDefaultHandler(aggmetrics, metricIndex, usage, "BenchmarkProcess")

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
