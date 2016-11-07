package in

import (
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/raintank/met/helper"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/metrictank/usage"
	"gopkg.in/raintank/schema.v1"
)

func BenchmarkHandle(b *testing.B) {
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	mdata.CluStatus = mdata.NewClusterStatus("default", false)
	mdata.InitMetrics(stats)

	store := mdata.NewDevnullStore()
	aggmetrics := mdata.NewAggMetrics(store, 600, 10, 800, 8000, 10000, 0, make([]mdata.AggSetting, 0))
	metricIndex := memory.New()
	metricIndex.Init(stats)
	usage := usage.New(300, aggmetrics, metricIndex, clock.New())
	in := New(aggmetrics, metricIndex, usage, "test", stats)

	// timestamps start at 10 and go up from there. (we can't use 0, see AggMetric.Add())
	datas := make([][]byte, b.N)
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
		data, err := metric.MarshalMsg(nil)
		if err != nil {
			b.Fatal(err.Error())
		}
		datas[i] = data
	}

	b.ResetTimer()
	go func() {
		for range chunk.TotalPoints {
		}
	}()
	for i := 0; i < b.N; i++ {
		in.Handle(datas[i])
	}
}
