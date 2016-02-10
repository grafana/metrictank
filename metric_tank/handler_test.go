package main

import (
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met/helper"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/msg"
	"github.com/raintank/raintank-metric/schema"
	"testing"
)

func BenchmarkHandler_HandleMessage1000(b *testing.B) {
	stats, _ := helper.New(false, "", "standard", "metrics_tank", "")
	clusterStatus = NewClusterStatus("default", false)
	initMetrics(stats)

	store := NewDevnullStore()
	aggmetrics := NewAggMetrics(store, 600, 10, 800, 8000, 10000, make([]aggSetting, 0))
	defCache := NewDefCache(metricdef.NewDefsMock())
	handler := NewHandler(aggmetrics, defCache)

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
			Time:       1234567890 + int64(i),
			TargetType: "gauge",
			Tags:       []string{"some_tag", "ok"},
		}
	}
	msgs := make([]*nsq.Message, 1000)
	for i := 0; i < len(msgs); i++ {
		id := nsq.MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
		for j := 0; j < len(metrics); j++ {
			metrics[j].Time += 10
		}
		data, err := msg.CreateMsg(metrics, 1, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			panic(err)
		}
		msgs[i] = nsq.NewMessage(id, data)
	}

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < len(msgs); i++ {
			err := handler.HandleMessage(msgs[i])
			if err != nil {
				panic(err)
			}
		}
	}
}
