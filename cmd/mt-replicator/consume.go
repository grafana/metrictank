package main

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type Consumer struct {
	consumer *cluster.Consumer
	Done     chan struct{}
}

func NewConsumer(brokers []string, group, topic string) (*Consumer, error) {
	config := cluster.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.ClientID = "mt-replicator"
	config.Group.Return.Notifications = true
	config.ChannelBufferSize = 1000
	config.Consumer.Fetch.Min = 1
	config.Consumer.Fetch.Default = 32768
	config.Consumer.MaxWaitTime = time.Second
	config.Consumer.MaxProcessingTime = time.Second
	config.Config.Version = sarama.V0_10_0_0
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	consumer, err := cluster.NewConsumer(brokers, group, []string{topic}, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer: consumer,
		Done:     make(chan struct{}),
	}, nil
}

func (c *Consumer) Consume(publisher *Publisher) {
	buf := make([]*schema.MetricData, 0)
	ticker := time.NewTicker(time.Second * 10)
	counter := 0
	counterTs := time.Now()
	msgChan := c.consumer.Messages()
	defer close(c.Done)
	for {
		select {
		case m, ok := <-msgChan:
			if !ok {
				return
			}
			md := &schema.MetricData{}
			_, err := md.UnmarshalMsg(m.Value)
			if err != nil {
				log.Error(3, "kafka-mdm decode error, skipping message. %s", err)
				continue
			}
			counter++
			buf = append(buf, md)
			if len(buf) > 1000 {
				log.Debug("flushing metricData buffer to kafka.")
				complete := false
				for !complete {
					if err = publisher.Send(buf); err != nil {
						log.Error(3, "failed to publish %d metrics. trying again in 1second", len(buf))
						time.Sleep(time.Second)
					} else {
						complete = true
					}
				}
				buf = buf[:0]
				c.consumer.MarkPartitionOffset(m.Topic, m.Partition, m.Offset, "")
			}
		case t := <-ticker.C:
			log.Info("%d metrics procesed in last %.1fseconds.", counter, t.Sub(counterTs).Seconds())
			counter = 0
			counterTs = t
		}
	}

}

func (c *Consumer) Stop() {
	c.consumer.Close()
}

func (c *Consumer) Start(publisher *Publisher) {
	go c.Consume(publisher)
}

func (c *Consumer) kafkaNotifications() {
	for msg := range c.consumer.Notifications() {
		if len(msg.Claimed) > 0 {
			for topic, partitions := range msg.Claimed {
				log.Info("kafka consumer claimed %d partitions on topic: %s", len(partitions), topic)
			}
		}
		if len(msg.Released) > 0 {
			for topic, partitions := range msg.Released {
				log.Info("kafka consumer released %d partitions on topic: %s", len(partitions), topic)
			}
		}

		if len(msg.Current) == 0 {
			log.Info("kafka consumer is no longer consuming from any partitions.")
		} else {
			log.Info("kafka Current partitions:")
			for topic, partitions := range msg.Current {
				log.Info("kafka Current partitions: %s: %v", topic, partitions)
			}
		}
	}
	log.Info("kafka notification processing stopped")
}
