package in

import (
	"fmt"
	"time"

	"github.com/raintank/met"
	"github.com/raintank/metrictank/defcache"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v0"
	"gopkg.in/raintank/schema.v0/msg"
)

// In is a base handler for a metrics packet, aimed to be embedded by concrete implementations
type In struct {
	metricsPerMessage met.Meter
	metricsReceived   met.Count
	msgsAge           met.Meter // in ms
	tmp               msg.MetricData

	metrics  mdata.Metrics
	defCache *defcache.DefCache
	usage    *usage.Usage
}

func New(metrics mdata.Metrics, defCache *defcache.DefCache, usage *usage.Usage, input string, stats met.Backend) In {
	return In{
		metricsPerMessage: stats.NewMeter(fmt.Sprintf("%s.metrics_per_message", input), 0),
		metricsReceived:   stats.NewCount(fmt.Sprintf("%s.metrics_received", input)),
		msgsAge:           stats.NewMeter(fmt.Sprintf("%s.message_age", input), 0),
		tmp:               msg.MetricData{Metrics: make([]*schema.MetricData, 1)},

		metrics:  metrics,
		defCache: defCache,
		usage:    usage,
	}
}

func (in In) process(metric *schema.MetricData) {
	if metric == nil {
		return
	}
	if metric.Id == "" {
		log.Error(3, "empty metric.Id - fix your datastream")
		return
	}
	if metric.Time == 0 {
		log.Warn("invalid metric. metric.Time is 0. %s", metric.Id)
	} else {
		in.defCache.Add(metric)
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
		Name:       name,
		Interval:   interval,
		Value:      val,
		Unit:       "unknown",
		Time:       int64(ts),
		TargetType: "gauge",
		Tags:       []string{},
		OrgId:      1, // admin org
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
		log.Error(3, "skipping message. %s", err)
		return
	}
	in.metricsPerMessage.Value(int64(1))
	in.metricsReceived.Inc(1)
	in.process(&md)
}

// HandleArray processes MetricDataArray messages that have a format spec and produced timestamp.
func (in In) HandleArray(data []byte) {
	err := in.tmp.InitFromMsg(data)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return
	}
	in.msgsAge.Value(time.Now().Sub(in.tmp.Produced).Nanoseconds() / 1000)

	err = in.tmp.DecodeMetricData() // reads metrics from in.tmp.Msg and unsets it
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return
	}
	in.metricsPerMessage.Value(int64(len(in.tmp.Metrics)))
	in.metricsReceived.Inc(int64(len(in.tmp.Metrics)))

	for _, metric := range in.tmp.Metrics {
		in.process(metric)
	}
}
