// Package in provides interfaces, concrete implementations, and utilities
// to ingest data into metrictank
package in

import (
	"fmt"

	"github.com/raintank/met"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

// In is a base handler for a metrics packet, aimed to be embedded by concrete implementations
type In struct {
	metricsPerMessage met.Meter
	metricsReceived   met.Count
	MetricsDecodeErr  met.Count // metric metrics_decode_err is a count of times an input message (MetricData, MetricDataArray or carbon line) failed to parse
	MetricInvalid     met.Count // metric metric_invalid is a count of times a metric did not validate
	msgsAge           met.Meter // in ms
	tmp               msg.MetricData

	metrics     mdata.Metrics
	metricIndex idx.MetricIndex
	usage       *usage.Usage
}

func New(metrics mdata.Metrics, metricIndex idx.MetricIndex, usage *usage.Usage, input string, stats met.Backend) In {
	return In{
		metricsPerMessage: stats.NewMeter(fmt.Sprintf("%s.metrics_per_message", input), 0),
		metricsReceived:   stats.NewCount(fmt.Sprintf("%s.metrics_received", input)),
		MetricsDecodeErr:  stats.NewCount(fmt.Sprintf("%s.metrics_decode_err", input)),
		MetricInvalid:     stats.NewCount(fmt.Sprintf("%s.metric_invalid", input)),
		msgsAge:           stats.NewMeter(fmt.Sprintf("%s.message_age", input), 0),
		tmp:               msg.MetricData{Metrics: make([]*schema.MetricData, 1)},

		metrics:     metrics,
		metricIndex: metricIndex,
		usage:       usage,
	}
}

func (in In) process(metric *schema.MetricData) {
	if metric == nil {
		return
	}
	err := metric.Validate()
	if err != nil {
		in.MetricInvalid.Inc(1)
		log.Debug("Invalid metric %s %v", err, metric)
		return
	}
	if metric.Time == 0 {
		log.Warn("invalid metric. metric.Time is 0. %s", metric.Id)
	} else {
		in.metricIndex.Add(metric)
		m := in.metrics.GetOrCreate(metric.Id)
		m.Add(uint32(metric.Time), metric.Value)
		if in.usage != nil {
			in.usage.Add(metric.OrgId, metric.Id)
		}
	}
}

// HandleLegacy processes legacy datapoints. we don't track msgsAge here
func (in In) HandleLegacy(name string, val float64, ts uint32, interval int) {
	// TODO reuse?
	md := &schema.MetricData{
		Name:     name,
		Metric:   name,
		Interval: interval,
		Value:    val,
		Unit:     "unknown",
		Time:     int64(ts),
		Mtype:    "gauge",
		Tags:     []string{},
		OrgId:    1, // admin org
	}
	md.SetId()
	in.metricsPerMessage.Value(int64(1))
	in.metricsReceived.Inc(1)
	in.process(md)
}

// Handle processes simple messages without format spec or produced timestamp, so we don't track msgsAge here
func (in In) Handle(data []byte) {
	// TODO reuse?
	md := schema.MetricData{}
	_, err := md.UnmarshalMsg(data)
	if err != nil {
		in.MetricsDecodeErr.Inc(1)
		log.Error(3, "skipping message. %s", err)
		return
	}
	in.metricsPerMessage.Value(int64(1))
	in.metricsReceived.Inc(1)
	in.process(&md)
}
