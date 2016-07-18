package mdata

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/raintank/met"
	cfg "github.com/raintank/metrictank/mdata/clkafka"
	"github.com/raintank/worldping-api/pkg/log"
)

type ClKafka struct {
	in       chan SavedChunk
	buf      []SavedChunk
	wg       sync.WaitGroup
	instance string
	consumer *cluster.Consumer
	producer sarama.SyncProducer
	StopChan chan int
	Cl
}

func NewKafka(instance string, metrics Metrics, stats met.Backend) *ClKafka {
	consumer, err := cluster.NewConsumer(cfg.Brokers, cfg.Group, cfg.Topics, cfg.CConfig)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to start consumer: %s", err)
	}
	log.Info("kafka-cluster consumer started without error")

	producer, err := sarama.NewSyncProducer(cfg.Brokers, cfg.PConfig)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to start producer: %s", err)
	}

	c := ClKafka{
		in:       make(chan SavedChunk),
		consumer: consumer,
		producer: producer,
		instance: instance,
		Cl: Cl{
			instance: instance,
			metrics:  metrics,
		},
		StopChan: make(chan int),
	}
	go c.notifications()
	go c.consume()
	go c.produce()

	return &c
}

func (c *ClKafka) consume() {
	c.wg.Add(1)
	messageChan := c.consumer.Messages()
	for msg := range messageChan {
		if LogLevel < 2 {
			log.Debug("CLU kafka-cluster received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
		}
		c.Handle(msg.Value)
		c.consumer.MarkOffset(msg, "")
	}
	log.Info("CLU kafka-cluster consumer ended.")
	c.wg.Done()
}

func (c *ClKafka) notifications() {
	c.wg.Add(1)
	for msg := range c.consumer.Notifications() {
		if len(msg.Claimed) > 0 {
			for topic, partitions := range msg.Claimed {
				log.Info("CLU kafka-cluster consumer claimed %d partitions on topic: %s", len(partitions), topic)
			}
		}
		if len(msg.Released) > 0 {
			for topic, partitions := range msg.Released {
				log.Info("CLU kafka-cluster consumer released %d partitions on topic: %s", len(partitions), topic)
			}
		}

		if len(msg.Current) == 0 {
			log.Info("CLU kafka-cluster consumer is no longer consuming from any partitions.")
		} else {
			log.Info("CLU kafka-cluster Current partitions:")
			for topic, partitions := range msg.Current {
				log.Info("CLU kafka-cluster Current partitions: %s: %v", topic, partitions)
			}
		}
	}
	log.Info("CLU kafka-cluster notification processing stopped")
	c.wg.Done()
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (c *ClKafka) Stop() {
	// closes notifications and messages channels, amongst others
	c.consumer.Close()
	c.producer.Close()

	go func() {
		c.wg.Wait()
		close(c.StopChan)
	}()
}

func (c *ClKafka) Send(sc SavedChunk) {
	c.in <- sc
}

func (c *ClKafka) produce() {
	ticker := time.NewTicker(time.Second)
	max := 5000
	for {
		select {
		case chunk := <-c.in:
			c.buf = append(c.buf, chunk)
			if len(c.buf) == max {
				c.flush()
			}
		case <-ticker.C:
			c.flush()
		}
	}
}

// flush makes sure the batch gets sent, asynchronously.
func (c *ClKafka) flush() {
	if len(c.buf) == 0 {
		return
	}

	msg := PersistMessageBatch{Instance: c.instance, SavedChunks: c.buf}
	c.buf = nil

	go func() {
		log.Debug("CLU kafka-cluster sending %d batch metricPersist messages", len(msg.SavedChunks))

		data, err := json.Marshal(&msg)
		if err != nil {
			log.Fatal(4, "CLU kafka-cluster failed to marshal persistMessage to json.")
		}
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint8(PersistMessageBatchV1))
		buf.Write(data)
		messagesSize.Value(int64(buf.Len()))
		payload := &sarama.ProducerMessage{
			Topic: cfg.Topic,
			Value: sarama.ByteEncoder(buf.Bytes()),
		}

		sent := false
		for !sent {
			// note: currently we don't do partitioning yet for cluster msgs, so no key needed
			_, _, err := c.producer.SendMessage(payload)
			if err != nil {
				log.Warn("CLU kafka-cluster publisher %s", err)
			} else {
				sent = true
			}
			time.Sleep(time.Second)
		}
		messagesPublished.Inc(1)
	}()
}
