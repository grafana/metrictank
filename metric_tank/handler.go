package main

import (
	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metric_tank/mdata"
	"github.com/raintank/raintank-metric/metric_tank/usage"
	"github.com/raintank/raintank-metric/msg"
	"time"
)

type Handler struct {
	metrics  mdata.Metrics
	defCache *defcache.DefCache
	usage    *usage.Usage
}

func NewHandler(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage) *Handler {
	return &Handler{
		metrics:  metrics,
		defCache: defCache,
		usage:    usg,
	}
}

func (h *Handler) HandleMessage(m *nsq.Message) error {
	ms, err := msg.MetricDataFromMsg(m.Body) // note: ms.Msg links to m.Body
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	msgsAge.Value(time.Now().Sub(ms.Produced).Nanoseconds() / 1000)

	err = ms.DecodeMetricData() // reads metrics from ms.Msg and unsets it
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	metricsPerMessage.Value(int64(len(ms.Metrics)))

	metricsReceived.Inc(int64(len(ms.Metrics)))

	for _, metric := range ms.Metrics {
		if metric.Id == "" {
			log.Fatal(3, "empty metric.Id - fix your datastream")
		}
		if metric.Time == 0 {
			log.Warn("invalid metric. metric.Time is 0. %s", metric.Id)
		} else {
			h.defCache.Add(metric)
			m := h.metrics.GetOrCreate(metric.Id)
			m.Add(uint32(metric.Time), metric.Value)
			if h.usage != nil {
				h.usage.Add(metric.OrgId, metric.Id)
			}
		}
	}

	return nil
}
