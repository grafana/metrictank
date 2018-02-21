package kafkamdm

import (
	"fmt"
	"testing"

	"gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

func generateMetricData(n int) []schema.DataPoint {
	datas := make([]schema.DataPoint, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("some.id.of.a.normal.metric.%d", i)
		metric := &schema.MetricData{
			OrgId:    500,
			Name:     name,
			Metric:   name,
			Interval: 10,
			Value:    1234.567,
			Unit:     "unkown",
			Time:     int64((i + 1) * 10),
			Mtype:    "gauge",
			Tags:     []string{"foo=bar", "host=server1", "dc=default"},
		}
		metric.SetId()
		datas[i] = metric
	}
	return datas
}

func generateMessages(points []schema.DataPoint) [][]byte {
	messages := make([][]byte, len(points))
	for i, p := range points {
		message, err := msg.DataPointToMsg(p, nil)
		if err != nil {
			panic(err)
		}
		messages[i] = message
	}
	return messages
}

func generateMetricPoint(n int) []schema.DataPoint {
	datas := generateMetricData(n)
	points := make([]schema.DataPoint, n)
	for i, md := range datas {
		points[i] = &schema.MetricPoint{
			Id:    md.GetId(),
			Time:  md.GetTime(),
			Value: md.GetValue(),
		}
	}

	return points
}

func BenchmarkDecodeMetricData(b *testing.B) {
	// generate messages
	msgCount := b.N
	if msgCount > 10000 {
		msgCount = 10000
	}
	msgs := generateMessages(generateMetricData(msgCount))
	k := KafkaMdm{
		Handler: MockHandler{},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k.handleMsg(msgs[i%msgCount], int32(b.N%32))
	}
}

func BenchmarkDecodeMetricPoint(b *testing.B) {
	msgCount := b.N
	if msgCount > 10000 {
		msgCount = 10000
	}
	msgs := generateMessages(generateMetricPoint(msgCount))
	k := KafkaMdm{
		Handler: MockHandler{},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k.handleMsg(msgs[i%msgCount], int32(b.N%32))
	}
}

type Handler interface {
	Process(metric schema.DataPoint, partition int32)
}

type MockHandler struct {
}

func (m MockHandler) Process(metrc schema.DataPoint, partition int32) {
	return
}
