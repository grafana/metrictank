package main

import (
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met/helper"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/msg"
	"github.com/raintank/raintank-metric/schema"
	"testing"
)

func BenchmarkHandler_HandleMessage(b *testing.B) {
	stats, _ := helper.New(false, "", "standard", "metrics_tank", "")
	clusterStatus = NewClusterStatus("default", false)
	initMetrics(stats)

	store := NewDevnullStore()
	aggmetrics := NewAggMetrics(store, 600, 10, 800, 8000, 10000, 0, make([]aggSetting, 0))
	defCache := defcache.New(metricdef.NewDefsMock(), stats)
	handler := NewHandler(aggmetrics, defCache, nil)

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
	// timestamps start at 1 and go up from there. (we can't use 0, see AggMetric.Add())
	msgs := make([]*nsq.Message, b.N)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := handler.HandleMessage(msgs[i])
		if err != nil {
			panic(err)
		}
	}
}
