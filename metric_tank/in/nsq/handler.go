package nsq

import (
	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metric_tank/mdata"
	"github.com/raintank/raintank-metric/metric_tank/usage"
	"github.com/raintank/raintank-metric/msg"
	"github.com/raintank/raintank-metric/schema"
)

type Handler struct {
	metrics  mdata.Metrics
	defCache *defcache.DefCache
	usage    *usage.Usage
	tmp      msg.MetricData
}

func NewHandler(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage) *Handler {
	return &Handler{
		metrics:  metrics,
		defCache: defCache,
		usage:    usg,
		tmp:      msg.MetricData{Metrics: make([]*schema.MetricData, 1)},
	}
}

func (h *Handler) HandleMessage(m *nsq.Message) error {
	err := h.tmp.InitFromMsg(m.Body)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	msgsAge.Value(time.Now().Sub(h.tmp.Produced).Nanoseconds() / 1000)

	err = h.tmp.DecodeMetricData() // reads metrics from h.tmp.Msg and unsets it
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	metricsPerMessage.Value(int64(len(h.tmp.Metrics)))

	metricsReceived.Inc(int64(len(h.tmp.Metrics)))

	for _, metric := range h.tmp.Metrics {
		if metric == nil {
			continue
		}
		if metric.Id == "" {
			log.Error(3, "empty metric.Id - fix your datastream")
			continue
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
