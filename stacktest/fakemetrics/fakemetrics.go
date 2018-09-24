package fakemetrics

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/grafana/metrictank/clock"
	"github.com/grafana/metrictank/stacktest/fakemetrics/out"
	"github.com/grafana/metrictank/stacktest/fakemetrics/out/carbon"
	"github.com/grafana/metrictank/stacktest/fakemetrics/out/kafkamdm"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/schema"
)

func generateMetrics(num int) []*schema.MetricData {
	metrics := make([]*schema.MetricData, num)

	for i := 0; i < num; i++ {
		name := fmt.Sprintf("some.id.of.a.metric.%d", i)
		m := &schema.MetricData{
			OrgId:    1,
			Name:     name,
			Interval: 1,
			Value:    1,
			Unit:     "s",
			Mtype:    "gauge",
		}
		m.SetId()
		metrics[i] = m
	}

	return metrics
}

type FakeMetrics struct {
	o       out.Out
	metrics []*schema.MetricData
	close   chan struct{}
	closed  bool
}

func NewFakeMetrics(metrics []*schema.MetricData, o out.Out, stats met.Backend) *FakeMetrics {
	fm := &FakeMetrics{
		o:       o,
		metrics: metrics,
		close:   make(chan struct{}),
	}
	go fm.run()
	return fm
}

func NewKafka(num int) *FakeMetrics {
	stats, _ := helper.New(false, "", "standard", "", "")
	out, err := kafkamdm.New("mdm", []string{"localhost:9092"}, "none", stats, "lastNum")
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("failed to create kafka-mdm output")
	}
	return NewFakeMetrics(generateMetrics(num), out, stats)
}

func NewCarbon(num int) *FakeMetrics {
	stats, _ := helper.New(false, "", "standard", "", "")
	out, err := carbon.New("localhost:2003", stats)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("failed to create kafka-mdm output")
	}
	return NewFakeMetrics(generateMetrics(num), out, stats)
}

func (f *FakeMetrics) Close() error {
	if f.closed {
		return nil
	}
	f.close <- struct{}{}
	return f.o.Close()
}

func (f *FakeMetrics) run() {
	// advantage over regular ticker:
	// 1) no ticks dropped
	// 2) ticks come asap after the start of a new second, so we can measure better how long it took to get the data
	ticker := clock.AlignedTick(time.Second)

	for {
		select {
		case <-f.close:
			return
		case tick := <-ticker:
			unix := tick.Unix()
			for i := range f.metrics {
				f.metrics[i].Time = unix
			}
			err := f.o.Flush(f.metrics)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Panic("failed to send data to output")
			}
		}
	}
}
