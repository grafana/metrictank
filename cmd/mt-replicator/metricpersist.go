package main

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/raintank/worldping-api/pkg/log"
)

type PersistReplicator struct {
	consumer  *cluster.Consumer
	producer  sarama.SyncProducer
	destTopic string
	done      chan struct{}
}

func NewPersistReplicator(srcBrokers []string, dstBrokers []string, group, srcTopic, destTopic string, initialOffset int) (*PersistReplicator, error) {
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

	return &PersistReplicator{
		consumer:  consumer,
		producer:  producer,
		destTopic: destTopic,
		done:      make(chan struct{}),
	}, nil
}

func (r *PersistReplicator) Consume() {
	accountingTicker := time.NewTicker(time.Second * 10)
	flushTicker := time.NewTicker(time.Second)
	counter := 0
	counterTs := time.Now()
	msgChan := r.consumer.Messages()
	var m *sarama.ConsumerMessage
	var ok bool

	buf := make([]*sarama.ProducerMessage, 0)
	defer close(r.done)
	for {
		select {
		case m, ok = <-msgChan:
			if !ok {
				if len(buf) != 0 {
					r.Flush(buf)
				}
				return
			}
			log.Debug("received metricPersist message with key: %s", m.Key)
			msg := &sarama.ProducerMessage{
				Key:   sarama.ByteEncoder(m.Key),
				Topic: r.destTopic,
				Value: sarama.ByteEncoder(m.Value),
			}
			buf = append(buf, msg)
			if len(buf) >= 1000 {
				r.Flush(buf)
				counter += len(buf)
				buf = buf[:0]
				r.consumer.MarkPartitionOffset(m.Topic, m.Partition, m.Offset, "")
			}
		case <-flushTicker.C:
			if len(buf) == 0 {
				continue
			}
			r.Flush(buf)
			counter += len(buf)
			buf = buf[:0]
			r.consumer.MarkPartitionOffset(m.Topic, m.Partition, m.Offset, "")
		case t := <-accountingTicker.C:
			log.Info("%d metricpersist messages processed in last %.1fseconds.", counter, t.Sub(counterTs).Seconds())
			counter = 0
			counterTs = t
		}
	}

}

func (r *PersistReplicator) Flush(buf []*sarama.ProducerMessage) {
	for {
		err := r.producer.SendMessages(buf)
		if err != nil {
			log.Error(3, "failed to publish metricPersist message. %s . trying again in 1second", err)
			time.Sleep(time.Second)
		} else {
			return
		}
	}
}

func (r *PersistReplicator) Stop() {
	r.consumer.Close()
	<-r.done
	r.producer.Close()
}

func (r *PersistReplicator) Start() {
	go r.Consume()
}
