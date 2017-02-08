package main

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/raintank/worldping-api/pkg/log"
)

type PersistRelay struct {
	consumer  *cluster.Consumer
	producer  sarama.SyncProducer
	destTopic string
	Done      chan struct{}
}

func NewPersistRelay(srcBrokers []string, dstBrokers []string, group, srcTopic, destTopic string, initialOffset int) (*PersistRelay, error) {
	config := cluster.NewConfig()
	config.Consumer.Offsets.Initial = int64(initialOffset)
	config.ClientID = "mt-persist-replicator"
	config.Group.Return.Notifications = true
	config.ChannelBufferSize = 1000
	config.Consumer.Fetch.Min = 1
	config.Consumer.Fetch.Default = 32768
	config.Consumer.MaxWaitTime = time.Second
	config.Consumer.MaxProcessingTime = time.Second
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = sarama.CompressionSnappy
	config.Config.Version = sarama.V0_10_0_0
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	consumer, err := cluster.NewConsumer(srcBrokers, fmt.Sprintf("%s-persist", group), []string{srcTopic}, config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(dstBrokers, &config.Config)
	if err != nil {
		return nil, err
	}

	return &PersistRelay{
		consumer:  consumer,
		producer:  producer,
		destTopic: destTopic,
		Done:      make(chan struct{}),
	}, nil
}

func (c *PersistRelay) Consume() {
	ticker := time.NewTicker(time.Second * 10)
	counter := 0
	counterTs := time.Now()
	msgChan := c.consumer.Messages()
	complete := false
	defer close(c.Done)
	for {
		select {
		case m, ok := <-msgChan:
			if !ok {
				return
			}
			log.Debug("received metricPersist message with key: %s", m.Key)
			msg := &sarama.ProducerMessage{
				Key:   sarama.ByteEncoder(m.Key),
				Topic: c.destTopic,
				Value: sarama.ByteEncoder(m.Value),
			}
			complete = false
			for !complete {
				_, _, err := c.producer.SendMessage(msg)
				if err != nil {
					log.Error(3, "failed to publish metricPersist message. %s . trying again in 1second", err)
					time.Sleep(time.Second)
				} else {
					complete = true
				}
			}
			counter++
			c.consumer.MarkPartitionOffset(m.Topic, m.Partition, m.Offset, "")
		case t := <-ticker.C:
			log.Info("%d metricpersist messages procesed in last %.1fseconds.", counter, t.Sub(counterTs).Seconds())
			counter = 0
			counterTs = t
		}
	}

}

func (c *PersistRelay) Stop() {
	c.consumer.Close()
	c.producer.Close()
}

func (c *PersistRelay) Start() {
	go c.Consume()
}

func (c *PersistRelay) kafkaNotifications() {
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
