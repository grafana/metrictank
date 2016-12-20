// Package in provides interfaces, concrete implementations, and utilities
// to ingest data into metrictank
package input

import (
	"fmt"

	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

// In is a base handler for a metrics packet, aimed to be embedded by concrete implementations
type Input struct {
	MetricsPerMessage *stats.Meter32
	metricsReceived   *stats.Counter32
	MetricsDecodeErr  *stats.Counter32 // metric metrics_decode_err is a count of times an input message (MetricData, MetricDataArray or carbon line) failed to parse
	MetricInvalid     *stats.Counter32 // metric metric_invalid is a count of times a metric did not validate
	MsgsAge           *stats.Meter32   // in ms

	metrics     mdata.Metrics
	metricIndex idx.MetricIndex
	usage       *usage.Usage
}

func New(metrics mdata.Metrics, metricIndex idx.MetricIndex, usage *usage.Usage, input string) Input {
	return Input{
		MetricsPerMessage: stats.NewMeter32(fmt.Sprintf("input.%s.metrics_per_message", input), false),
		metricsReceived:   stats.NewCounter32(fmt.Sprintf("input.%s.metrics_received", input)),
		MetricsDecodeErr:  stats.NewCounter32(fmt.Sprintf("input.%s.metrics_decode_err", input)),
		MetricInvalid:     stats.NewCounter32(fmt.Sprintf("input.%s.metric_invalid", input)),
		MsgsAge:           stats.NewMeter32(fmt.Sprintf("input.%s.message_age", input), false),

		metrics:     metrics,
		metricIndex: metricIndex,
		usage:       usage,
	}
}

// process makes sure the data is stored and the metadata is in the index,
// and the usage is tracked, if enabled.
// concurrency-safe.
func (in Input) Process(metric *schema.MetricData, partition int32) {
	if metric == nil {
		return
	}
	in.metricsReceived.Inc()
	err := metric.Validate()
	if err != nil {
		in.MetricInvalid.Inc()
		log.Debug("in: Invalid metric %s %v", err, metric)
		return
	}
	if metric.Time == 0 {
		log.Warn("in: invalid metric. metric.Time is 0. %s", metric.Id)
	} else {
		in.metricIndex.Add(metric, partition)
		m := in.metrics.GetOrCreate(metric.Id)
		m.Add(uint32(metric.Time), metric.Value)
		if in.usage != nil {
			in.usage.Add(metric.OrgId, metric.Id)
		}
	}
}
