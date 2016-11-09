// Package in provides interfaces, concrete implementations, and utilities
// to ingest data into metrictank
package input

import (
	"fmt"

	"github.com/raintank/met"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

// In is a base handler for a metrics packet, aimed to be embedded by concrete implementations
type Input struct {
	MetricsPerMessage met.Meter
	metricsReceived   met.Count
	MetricsDecodeErr  met.Count // metric metrics_decode_err is a count of times an input message (MetricData, MetricDataArray or carbon line) failed to parse
	MetricInvalid     met.Count // metric metric_invalid is a count of times a metric did not validate
	MsgsAge           met.Meter // in ms

	metrics     mdata.Metrics
	metricIndex idx.MetricIndex
	usage       *usage.Usage
}

func New(metrics mdata.Metrics, metricIndex idx.MetricIndex, usage *usage.Usage, input string, stats met.Backend) Input {
	return Input{
		MetricsPerMessage: stats.NewMeter(fmt.Sprintf("%s.metrics_per_message", input), 0),
		metricsReceived:   stats.NewCount(fmt.Sprintf("%s.metrics_received", input)),
		MetricsDecodeErr:  stats.NewCount(fmt.Sprintf("%s.metrics_decode_err", input)),
		MetricInvalid:     stats.NewCount(fmt.Sprintf("%s.metric_invalid", input)),
		MsgsAge:           stats.NewMeter(fmt.Sprintf("%s.message_age", input), 0),

		metrics:     metrics,
		metricIndex: metricIndex,
		usage:       usage,
	}
}

// process makes sure the data is stored and the metadata is in the index,
// and the usage is tracked, if enabled.
// concurrency-safe.
func (in Input) Process(metric *schema.MetricData) {
	if metric == nil {
		return
	}
	in.metricsReceived.Inc(1)
	err := metric.Validate()
	if err != nil {
		in.MetricInvalid.Inc(1)
		log.Debug("in: Invalid metric %s %v", err, metric)
		return
	}
	if metric.Time == 0 {
		log.Warn("in: invalid metric. metric.Time is 0. %s", metric.Id)
	} else {
		in.metricIndex.Add(metric)
		m := in.metrics.GetOrCreate(metric.Id)
		m.Add(uint32(metric.Time), metric.Value)
		if in.usage != nil {
			in.usage.Add(metric.OrgId, metric.Id)
		}
	}
}
