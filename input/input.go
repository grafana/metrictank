// Package in provides interfaces, concrete implementations, and utilities
// to ingest data into metrictank
package input

import (
	"fmt"
	"sync"
	"time"

	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type Handler interface {
	Process(metric schema.DataPoint, partition int32)
}

// TODO: clever way to document all metrics for all different inputs

// Default is a base handler for a metrics packet, aimed to be embedded by concrete implementations
type DefaultHandler struct {
	metricsReceived *stats.Counter32
	MetricInvalid   *stats.Counter32 // metric metric_invalid is a count of times a metric did not validate
	MsgsAge         *stats.Meter32   // in ms
	pressureIdx     *stats.Counter32
	pressureTank    *stats.Counter32

	metrics mdata.Metrics
}

type IntervalLookupRecord struct {
	sync.Mutex

	Interval   int
	DataPoints []*schema.MetricData
	Last       time.Time
	pos        int
}

func NewDefaultHandler(input string) *DefaultHandler {
	return &DefaultHandler{
		metricsReceived: stats.NewCounter32(fmt.Sprintf("input.%s.metrics_received", input)),
		MetricInvalid:   stats.NewCounter32(fmt.Sprintf("input.%s.metric_invalid", input)),
		MsgsAge:         stats.NewMeter32(fmt.Sprintf("input.%s.message_age", input), false),
		pressureIdx:     stats.NewCounter32(fmt.Sprintf("input.%s.pressure.idx", input)),
		pressureTank:    stats.NewCounter32(fmt.Sprintf("input.%s.pressure.tank", input)),
	}
}

// process makes sure the data is stored and the metadata is in the index
// concurrency-safe.
func (in *DefaultHandler) Process(metric schema.DataPoint, partition int32) {
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
	if metric.GetTime() == 0 {
		in.MetricInvalid.Inc()
		log.Warn("in: invalid metric. metric.Time is 0. %s", metric.GetId())
		return
	}
	pre := time.Now()
	mdata.MemoryStore.StoreDataPoint(metric, partition)
	in.pressureTank.Add(int(time.Since(pre).Nanoseconds()))
}
