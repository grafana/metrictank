package insq

import (
	"fmt"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/raintank/metrictank/stats"
)

type Consumer struct {
	*nsq.Consumer
	msgsReceived    *stats.Counter64
	msgsFinished    *stats.Counter64
	msgsRequeued    *stats.Counter64
	msgsConnections *stats.Gauge32
	numHandlers     *stats.Gauge32
}

func NewConsumer(topic, channel string, config *nsq.Config, metricsPatt string) (*Consumer, error) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return nil, err
	}
	c := Consumer{
		consumer,
		stats.NewCounter64(fmt.Sprintf(metricsPatt, "received")),
		stats.NewCounter64(fmt.Sprintf(metricsPatt, "finished")),
		stats.NewCounter64(fmt.Sprintf(metricsPatt, "requeued")),
		stats.NewGauge32(fmt.Sprintf(metricsPatt, "connections")),
		stats.NewGauge32(fmt.Sprintf(metricsPatt, "num_handlers")),
	}
	go func() {
		for range time.Tick(time.Second) {
			s := consumer.Stats()
			c.msgsReceived.SetUint64(s.MessagesReceived)
			c.msgsFinished.SetUint64(s.MessagesFinished)
			c.msgsRequeued.SetUint64(s.MessagesRequeued)
			c.msgsConnections.Set(s.Connections)
		}
	}()
	return &c, nil
}

func (r *Consumer) AddConcurrentHandlers(handler nsq.Handler, concurrency int) {
	r.numHandlers.Add(concurrency)
	r.Consumer.AddConcurrentHandlers(handler, concurrency)
}
