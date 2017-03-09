package input

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	whisper "github.com/lomik/go-whisper"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/cache"
	"github.com/raintank/metrictank/usage"
	"gopkg.in/raintank/schema.v1"
)

func BenchmarkProcess(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := mdata.NewDevnullStore()

	mdata.SetSingleSchema(whisper.NewRetentionMT(10, 10000, 600, 10, true))
	mdata.SetOnlyDefaultAgg(whisper.Average, whisper.Min, whisper.Max)
	mdata.Cache(time.Minute, time.Hour)

	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, 800, 8000, 0)
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
