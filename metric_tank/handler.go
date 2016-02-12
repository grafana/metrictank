package main

import (
	//	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/msg"
	"sync"
	"time"
)

var bufpool = sync.Pool{
	//New: func() interface{} { return new(bytes.Buffer) },
	New: func() interface{} { return make([]byte, 0, 10000) }, // TODO optimize default size based on emperical sizing measurements
}

type Handler struct {
	metrics  Metrics
	defCache *DefCache
}

func NewHandler(metrics Metrics, defCache *DefCache) *Handler {
	return &Handler{
		metrics:  metrics,
		defCache: defCache,
	}
}

func (h *Handler) HandleMessage(m *nsq.Message) error {
	ms, err := msg.MetricDataFromMsg(m.Body)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	msgsAge.Value(time.Now().Sub(ms.Produced).Nanoseconds() / 1000)

	buf := bufpool.Get().([]byte)
	//fmt.Println("buf", len(buf), cap(buf)) // 0 10k
	buf, err = ms.DecodeMetricData(buf)
	//fmt.Println("buf2", len(buf), cap(buf)) // 0 10k
	bufpool.Put(buf)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	metricsPerMessage.Value(int64(len(ms.Metrics)))

	metricsReceived.Inc(int64(len(ms.Metrics)))

	for _, metric := range ms.Metrics {
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
