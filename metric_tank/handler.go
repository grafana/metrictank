package main

import (
	//	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/msg"
	"time"
)

type Handler struct {
	metrics  Metrics
	defCache *DefCache
	buf      []byte
	bufMD    *msg.MetricData
}

func NewHandler(metrics Metrics, defCache *DefCache) *Handler {
	return &Handler{
		metrics:  metrics,
		defCache: defCache,
		buf:      make([]byte, 0, 10000),  // TODO optimize default size based on emperical sizing measurements
		bufMD:    msg.NewMetricData(1000), // ditto
	}
}

func (h *Handler) HandleMessage(m *nsq.Message) error {
	err := h.bufMD.InitializeFromMsg(m.Body)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	msgsAge.Value(time.Now().Sub(h.bufMD.Produced).Nanoseconds() / 1000)

	_, err = h.bufMD.DecodeMetricData(h.buf)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	metricsPerMessage.Value(int64(len(h.bufMD.Metrics)))

	metricsReceived.Inc(int64(len(h.bufMD.Metrics)))

	for _, metric := range h.bufMD.Metrics {
		if metric.Time == 0 {
			log.Warn("invalid metric. metric.Time is 0. %s", metric.GetId())
		} else {
			h.defCache.Add(metric)
			m := h.metrics.GetOrCreate(metric.GetId())
			m.Add(uint32(metric.Time), metric.Value)
		}
	}

	return nil
}
