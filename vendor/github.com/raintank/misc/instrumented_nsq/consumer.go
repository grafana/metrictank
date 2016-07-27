package insq

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
)

type Consumer struct {
	*nsq.Consumer
	msgsReceived    met.Gauge
	msgsFinished    met.Gauge
	msgsRequeued    met.Gauge
	msgsConnections met.Gauge
	numHandlers     int32
	handlers        met.Gauge
}

func NewConsumer(topic, channel string, config *nsq.Config, metricsPatt string, metrics met.Backend) (*Consumer, error) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	c := Consumer{
		consumer,
		metrics.NewGauge(fmt.Sprintf(metricsPatt, "received"), 0),
		metrics.NewGauge(fmt.Sprintf(metricsPatt, "finished"), 0),
		metrics.NewGauge(fmt.Sprintf(metricsPatt, "requeued"), 0),
		metrics.NewGauge(fmt.Sprintf(metricsPatt, "connections"), 0),
		0,
		metrics.NewGauge(fmt.Sprintf(metricsPatt, "num_handlers"), 0),
	}
	go func() {
		t := time.Tick(time.Second * time.Duration(1))
		for range t {
			s := consumer.Stats()
			c.msgsReceived.Value(int64(s.MessagesReceived))
			c.msgsFinished.Value(int64(s.MessagesFinished))
			c.msgsRequeued.Value(int64(s.MessagesRequeued))
			c.msgsConnections.Value(int64(s.Connections))
			h := atomic.LoadInt32(&c.numHandlers)
			c.handlers.Value(int64(h))
		}
	}()
	return &c, err
}

func (r *Consumer) AddConcurrentHandlers(handler nsq.Handler, concurrency int) {
	atomic.AddInt32(&r.numHandlers, int32(concurrency))
	r.Consumer.AddConcurrentHandlers(handler, concurrency)
}
