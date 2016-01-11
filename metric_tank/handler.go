package main

import (
	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/msg"
	"time"
)

type Handler struct {
	metrics   Metrics
	metaCache *MetaCache
}

func NewHandler(metrics Metrics, metaCache *MetaCache) *Handler {
	return &Handler{
		metrics:   metrics,
		metaCache: metaCache,
	}
}

func (h *Handler) HandleMessage(m *nsq.Message) error {
	ms, err := msg.MetricDataFromMsg(m.Body)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	msgsAge.Value(time.Now().Sub(ms.Produced).Nanoseconds() / 1000)

	err = ms.DecodeMetricData()
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	metricsPerMessage.Value(int64(len(ms.Metrics)))

	metricsReceived.Inc(int64(len(ms.Metrics)))

	go func() {
		pre := time.Now()
		for i, m := range ms.Metrics {
			if err := metricdef.EnsureIndex(m); err != nil {
				log.Error(3, "couldn't index to ES %s: %s", m.Id(), err)
				metricsToEsFail.Inc(int64(len(ms.Metrics) - i))
				return
			}
		}
		metricsToEsOK.Inc(int64(len(ms.Metrics)))
		esPutDuration.Value(time.Now().Sub(pre))
	}()

	for _, metric := range ms.Metrics {
		if metric.Time == 0 {
			log.Warn("invalid metric. metric.Time is 0. %s", metric.Id())
		} else {
			m := h.metrics.GetOrCreate(metric.Id())
			m.Add(uint32(metric.Time), metric.Value)
			metaCache.Add(metric.Id(), metric.Interval, metric.TargetType)
		}
	}

	return nil
}
