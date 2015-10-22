package main

import (
	"log"

	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/msg"
)

type Handler struct {
	metrics Metrics
}

func NewHandler(metrics Metrics) *Handler {
	return &Handler{
		metrics: metrics,
	}
}

func (h *Handler) HandleMessage(m *nsq.Message) error {
	ms, err := msg.MetricDataFromMsg(m.Body)
	if err != nil {
		log.Println("ERROR:", err, "skipping message")
		return nil
	}
	//	msgsAge.Value(time.Now().Sub(ms.Produced).Nanoseconds() / 1000)

	err = ms.DecodeMetricData()
	if err != nil {
		log.Println("ERROR:", err, "skipping message")
		return nil
	}
	//	metricsPerMessage.Value(int64(len(ms.Metrics)))

	metricsReceived.Inc(int64(len(ms.Metrics)))
	for _, metric := range ms.Metrics {
		m := h.metrics.GetOrCreate(metric.Id())
		m.Add(uint32(metric.Time), metric.Value)
	}

	return nil
}
