// Package in provides interfaces, concrete implementations, and utilities
// to ingest data into metrictank
package input

import (
	"fmt"
	"time"

	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type Handler interface {
	Process(metric *schema.MetricData, partition int32)
}

// TODO: clever way to document all metrics for all different inputs

// Default is a base handler for a metrics packet, aimed to be embedded by concrete implementations
type DefaultHandler struct {
	metricsReceived *stats.Counter32
	MetricInvalid   *stats.Counter32 // metric metric_invalid is a count of times a metric did not validate
	MsgsAge         *stats.Meter32   // in ms
	pressureIdx     *stats.Counter32
	pressureTank    *stats.Counter32

	metrics     mdata.Metrics
	metricIndex idx.MetricIndex
	usage       *usage.Usage
}

func NewDefaultHandler(metrics mdata.Metrics, metricIndex idx.MetricIndex, usage *usage.Usage, input string) DefaultHandler {
	return DefaultHandler{
		metricsReceived: stats.NewCounter32(fmt.Sprintf("input.%s.metrics_received", input)),
		MetricInvalid:   stats.NewCounter32(fmt.Sprintf("input.%s.metric_invalid", input)),
		MsgsAge:         stats.NewMeter32(fmt.Sprintf("input.%s.message_age", input), false),
		pressureIdx:     stats.NewCounter32(fmt.Sprintf("input.%s.pressure.idx", input)),
		pressureTank:    stats.NewCounter32(fmt.Sprintf("input.%s.pressure.tank", input)),

		metrics:     metrics,
		metricIndex: metricIndex,
		usage:       usage,
	}
}

// process makes sure the data is stored and the metadata is in the index,
// and the usage is tracked, if enabled.
// concurrency-safe.
func (in DefaultHandler) Process(metric *schema.MetricData, partition int32) {
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
		in.MetricInvalid.Inc()
		log.Warn("in: invalid metric. metric.Time is 0. %s", metric.Id)
		return
	}

	pre := time.Now()
	err = in.metricIndex.AddOrUpdate(metric, partition)
	in.pressureIdx.Add(int(time.Since(pre).Nanoseconds()))
	if err != nil {
		return
	}

	pre = time.Now()
	m := in.metrics.GetOrCreate(metric.Id)
	m.Add(uint32(metric.Time), metric.Value)
	if in.usage != nil {
		in.usage.Add(metric.OrgId, metric.Id)
	}
	in.pressureTank.Add(int(time.Since(pre).Nanoseconds()))
}
