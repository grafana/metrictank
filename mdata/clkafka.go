package mdata

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/kafka"
	cfg "github.com/raintank/metrictank/mdata/clkafka"
	"github.com/raintank/worldping-api/pkg/log"
)

type ClKafka struct {
	in        chan SavedChunk
	buf       []SavedChunk
	wg        sync.WaitGroup
	instance  string
	consumer  sarama.Consumer
	client    sarama.Client
	producer  sarama.SyncProducer
	offsetMgr *kafka.OffsetMgr
	StopChan  chan int
	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
	Cl
}

func NewKafka(instance string, metrics Metrics, stats met.Backend) *ClKafka {
	client, err := sarama.NewClient(cfg.Brokers, cfg.Config)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to start client: %s", err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to initialize consumer: %s", err)
	}
	log.Info("kafka-cluster consumer initialized without error")

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to initialize producer: %s", err)
	}

	offsetMgr, err := kafka.NewOffsetMgr(cfg.DataDir)
	if err != nil {
		log.Fatal(2, "kafka-cluster couldnt create offsetMgr. %s", err)
	}

	c := ClKafka{
		in:        make(chan SavedChunk),
		offsetMgr: offsetMgr,
		client:    client,
		consumer:  consumer,
		producer:  producer,
		instance:  instance,
		Cl: Cl{
			instance: instance,
			metrics:  metrics,
		},
		StopChan:      make(chan int),
		stopConsuming: make(chan struct{}),
	}
	c.start()
	go c.produce()

	return &c
}

func (c *ClKafka) start() {
	// get partitions.
	topic := cfg.Topic
	partitions, err := c.consumer.Partitions(topic)
	if err != nil {
		log.Fatal(4, "kafka-cluster: Faild to get partitions for topic %s. %s", topic, err)
	}
	for _, partition := range partitions {
		switch cfg.Offset {
		case "oldest":
			go c.consumePartition(topic, partition, -2)
		case "newest":
			go c.consumePartition(topic, partition, -1)
		case "last":
			o, err := c.offsetMgr.Last(topic, partition)
			if err != nil {
				log.Fatal(4, "kafka-cluster: Failed to get offset for %s:%d. %s", topic, partition, err)
			}
			go c.consumePartition(topic, partition, o)
		default:
			o, err := c.client.GetOffset(topic, partition, time.Now().Add(-1*cfg.OffsetDuration).UnixNano()/int64(time.Millisecond))
			if err != nil {
				log.Fatal(4, "kafka-mdm: failed to get offset for %s:%d.  %s", topic, partition, err)
			}
			go c.consumePartition(topic, partition, o)
		}
	}
}

func (c *ClKafka) consumePartition(topic string, partition int32, partitionOffset int64) {
	c.wg.Add(1)
	defer c.wg.Done()

	pc, err := c.consumer.ConsumePartition(topic, partition, partitionOffset)
	if err != nil {
		log.Fatal(4, "kafka-cluster: failed to start partitionConsumer for %s:%d. %s", topic, partition, err)
	}
	log.Info("kafka-cluster: consuming from %s:%d from offset %d", topic, partition, partitionOffset)
	currentOffset := partitionOffset
	messages := pc.Messages()
	ticker := time.NewTicker(cfg.OffsetCommitInterval)
	for {
		select {
		case msg := <-messages:
			if LogLevel < 2 {
				log.Debug("kafka-cluster received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
			}
			c.Handle(msg.Value)
			currentOffset = msg.Offset
		case <-ticker.C:
			if err := c.offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-cluster failed to commit offset for %s:%d, %s", topic, partition, err)
			}
		case <-c.stopConsuming:
			pc.Close()
			if err := c.offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-cluster failed to commit offset for %s:%d, %s", topic, partition, err)
			}
			log.Info("kafka-cluster consumer for %s:%d ended.", topic, partition)
			return
		}
	}
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (c *ClKafka) Stop() {
	// closes notifications and messages channels, amongst others
	close(c.stopConsuming)
	c.producer.Close()

	go func() {
		c.wg.Wait()
		c.offsetMgr.Close()
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
