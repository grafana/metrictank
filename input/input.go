// Package in provides interfaces, concrete implementations, and utilities
// to ingest data into metrictank
package input

import (
	"fmt"
	"time"

	schema "gopkg.in/raintank/schema.v1"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
)

type Handler interface {
	ProcessMetricData(md *schema.MetricData, partition int32)
	ProcessMetricPoint(point schema.MetricPoint, partition int32)
}

// TODO: clever way to document all metrics for all different inputs

// Default is a base handler for a metrics packet, aimed to be embedded by concrete implementations
type DefaultHandler struct {
	receivedMD   *stats.Counter32
	receivedMP   *stats.Counter32
	invalidMD    *stats.Counter32
	invalidMP    *stats.Counter32
	unknownMP    *stats.Counter32
	pressureIdx  *stats.Counter32
	pressureTank *stats.Counter32

	metrics     mdata.Metrics
	metricIndex idx.MetricIndex
}

func NewDefaultHandler(metrics mdata.Metrics, metricIndex idx.MetricIndex, input string) DefaultHandler {
	return DefaultHandler{
		receivedMD:   stats.NewCounter32(fmt.Sprintf("input.%s.metricdata.received", input)),
		receivedMP:   stats.NewCounter32(fmt.Sprintf("input.%s.metricpoint.received", input)),
		invalidMD:    stats.NewCounter32(fmt.Sprintf("input.%s.metricdata.invalid", input)),
		invalidMP:    stats.NewCounter32(fmt.Sprintf("input.%s.metricpoint.invalid", input)),
		unknownMP:    stats.NewCounter32(fmt.Sprintf("input.%s.metricpoint.unknown", input)),
		pressureIdx:  stats.NewCounter32(fmt.Sprintf("input.%s.pressure.idx", input)),
		pressureTank: stats.NewCounter32(fmt.Sprintf("input.%s.pressure.tank", input)),

		metrics:     metrics,
		metricIndex: metricIndex,
	}
}

// ProcessMetricPoint updates the index if possible, and stores the data if we have an index entry
// concurrency-safe.
func (in DefaultHandler) ProcessMetricPoint(point schema.MetricPoint, partition int32) {
	in.receivedMP.Inc()
	if !point.Valid() {
		in.invalidMP.Inc()
		log.Debug("in: Invalid metric %v", point)
		return
	}

	pre := time.Now()
	archive, _, ok := in.metricIndex.Update(point, partition)
	in.pressureIdx.Add(int(time.Since(pre).Nanoseconds()))

	if !ok {
		in.unknownMP.Inc()
		return
	}

	pre = time.Now()
	m := in.metrics.GetOrCreate(point.MKey, archive.SchemaId, archive.AggId)
	m.Add(point.Time, point.Value)
	in.pressureTank.Add(int(time.Since(pre).Nanoseconds()))

}

// ProcessMetricData assures the data is stored and the metadata is in the index
// concurrency-safe.
func (in DefaultHandler) ProcessMetricData(md *schema.MetricData, partition int32) {
	in.receivedMD.Inc()
	err := md.Validate()
	if err != nil {
		in.invalidMD.Inc()
		log.Debug("in: Invalid metric %v: %s", md, err)
		return
	}
	if md.Time == 0 {
		in.invalidMD.Inc()
		log.Warn("in: invalid metric. metric.Time is 0. %s", md.Id)
		return
	}

	mkey, err := schema.MKeyFromString(md.Id)
	if err != nil {
		log.Error(3, "in: Invalid metric %v: could not parse ID: %s", md, err)
		return
	}

	pre := time.Now()
	archive, _, _ := in.metricIndex.AddOrUpdate(mkey, md, partition)
	in.pressureIdx.Add(int(time.Since(pre).Nanoseconds()))

	pre = time.Now()
	m := in.metrics.GetOrCreate(mkey, archive.SchemaId, archive.AggId)
	m.Add(uint32(md.Time), md.Value)
	in.pressureTank.Add(int(time.Since(pre).Nanoseconds()))
}
