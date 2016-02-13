package msg

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/schema"
	"runtime"
	"testing"
)

func Benchmark_CreateMessage(b *testing.B) {
	metrics := make([]*schema.MetricData, 10)
	for i := 0; i < len(metrics); i++ {
		metrics[i] = &schema.MetricData{
			Id:         "some.id.of.a.metric",
			OrgId:      500,
			Name:       "some.id",
			Metric:     "metric",
			Interval:   60,
			Value:      1234.567,
			Unit:       "ms",
			Time:       int64(i - len(metrics) + 1),
			TargetType: "gauge",
			Tags:       []string{"some_tag", "ok"},
		}
	}
	b.ResetTimer()
	var id nsq.MessageID
	var m *nsq.Message
	for i := 0; i < b.N; i++ {
		id = nsq.MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
		for j := 0; j < len(metrics); j++ {
			metrics[j].Time += 10
		}
		data, err := CreateMsg(metrics, 1, FormatMetricDataArrayMsgp)
		if err != nil {
			panic(err)
		}
		m = nsq.NewMessage(id, data)
	}
	// prevents the go compiler from optimizing msgs away. presumably..
	if m.ID != id {
		panic("bad id")
	}
	b.StopTimer()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	if b.N == 1 {
		fmt.Println()
	}
	fmt.Printf("messages: %6d, system bytes needed: %10d\n", b.N, stats.Sys)
}
