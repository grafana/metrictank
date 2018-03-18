// Package in provides interfaces, concrete implementations, and utilities
// to ingest data into metrictank
package input

import (
	"fmt"
	"time"

	schema "gopkg.in/raintank/schema.v1"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/msg"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
)

type Handler interface {
	Process(pointMsg msg.Point, partition int32)
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
}

func NewDefaultHandler(metrics mdata.Metrics, metricIndex idx.MetricIndex, input string) DefaultHandler {
	return DefaultHandler{
		metricsReceived: stats.NewCounter32(fmt.Sprintf("input.%s.metrics_received", input)),
		MetricInvalid:   stats.NewCounter32(fmt.Sprintf("input.%s.metric_invalid", input)),
		MsgsAge:         stats.NewMeter32(fmt.Sprintf("input.%s.message_age", input), false),
		pressureIdx:     stats.NewCounter32(fmt.Sprintf("input.%s.pressure.idx", input)),
		pressureTank:    stats.NewCounter32(fmt.Sprintf("input.%s.pressure.tank", input)),

		metrics:     metrics,
		metricIndex: metricIndex,
	}
}

// process makes sure the data is stored and the metadata is in the index
// concurrency-safe.
func (in DefaultHandler) Process(pointMsg msg.Point, partition int32) {
	in.metricsReceived.Inc()
	var mkey schema.MKey
	var timestamp uint32
	var value float64

	if pointMsg.Val == 0 {
		err := pointMsg.Md.Validate()
		if err != nil {
			in.MetricInvalid.Inc()
			log.Debug("in: Invalid metric %v: %s", pointMsg.Md, err)
			return
		}
		if pointMsg.Md.Time == 0 {
			in.MetricInvalid.Inc()
			log.Warn("in: invalid metric. metric.Time is 0. %s", pointMsg.Md.Id)
			return
		}

		mkey, err = schema.MKeyFromString(pointMsg.Md.Id)
		if err != nil {
			log.Debug("in: Invalid metric %v: %s", pointMsg.Md, err)
		}

		timestamp = uint32(pointMsg.Md.Time)
		value = pointMsg.Md.Value
	} else {
		if !pointMsg.Point.Valid() {
			in.MetricInvalid.Inc()
			log.Debug("in: Invalid metric %v", pointMsg.Point)
			return
		}
		mkey = schema.MKey{
			Key: pointMsg.Point.MetricPointId1.Id,
			Org: pointMsg.Point.Org,
		}
		timestamp = pointMsg.Point.MetricPointId1.Time
		value = pointMsg.Point.MetricPointId1.Value

	}

	pre := time.Now()
	archive := in.metricIndex.AddOrUpdate(pointMsg, partition)
	in.pressureIdx.Add(int(time.Since(pre).Nanoseconds()))

	pre = time.Now()
	m := in.metrics.GetOrCreate(mkey, archive.SchemaId, archive.AggId)
	m.Add(timestamp, value)
	in.pressureTank.Add(int(time.Since(pre).Nanoseconds()))
}
