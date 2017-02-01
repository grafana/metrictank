package notifierKafka

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/kafka"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/util"
	"github.com/raintank/worldping-api/pkg/log"
)

type NotifierKafka struct {
	in        chan mdata.SavedChunk
	buf       []mdata.SavedChunk
	wg        sync.WaitGroup
	instance  string
	consumer  sarama.Consumer
	client    sarama.Client
	producer  sarama.SyncProducer
	offsetMgr *kafka.OffsetMgr
	StopChan  chan int
	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
	mdata.Notifier
	idx   idx.MetricIndex
	bPool *util.BufferPool
}

func New(instance string, metrics mdata.Metrics, idx idx.MetricIndex) *NotifierKafka {
	client, err := sarama.NewClient(brokers, config)
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

	offsetMgr, err := kafka.NewOffsetMgr(dataDir)
	if err != nil {
		log.Fatal(2, "kafka-cluster couldnt create offsetMgr. %s", err)
	}

	c := NotifierKafka{
		in:        make(chan mdata.SavedChunk),
		offsetMgr: offsetMgr,
		client:    client,
		consumer:  consumer,
		producer:  producer,
		instance:  instance,
		Notifier: mdata.Notifier{
			Instance:             instance,
			Metrics:              metrics,
			CreateMissingMetrics: true,
		},
		StopChan:      make(chan int),
		stopConsuming: make(chan struct{}),
		idx:           idx,
		bPool:         util.NewBufferPool(),
	}
	c.start()
	go c.produce()

	return &c
}

func (c *NotifierKafka) start() {
	var err error
	pre := time.Now()
	processBacklog := new(sync.WaitGroup)
	for _, partition := range partitions {
		var offset int64
		switch offsetStr {
		case "oldest":
			offset = -2
		case "newest":
			offset = -1
		case "last":
			offset, err = c.offsetMgr.Last(topic, partition)
		default:
			offset, err = c.client.GetOffset(topic, partition, time.Now().Add(-1*offsetDuration).UnixNano()/int64(time.Millisecond))
		}
		if err != nil {
			log.Fatal(4, "kafka-cluster: Failed to get %q duration offset for %s:%d. %q", offsetStr, topic, partition, err)
		}
		partitionLogSize[partition].Set(int(bootTimeOffsets[partition]))
		if offset >= 0 {
			partitionOffset[partition].Set(int(offset))
			partitionLag[partition].Set(int(bootTimeOffsets[partition] - offset))
		}
		processBacklog.Add(1)
		go c.consumePartition(topic, partition, offset, processBacklog)
	}
	// wait for our backlog to be processed before returning.  This will block metrictank from consuming metrics until
	// we have processed old metricPersist messages. The end result is that we wont overwrite chunks in cassandra that
	// have already been previously written.
	// We don't wait more than backlogProcessTimeout for the backlog to be processed.
	log.Info("kafka-cluster: waiting for metricPersist backlog to be processed.")
	backlogProcessed := make(chan struct{}, 1)
	go func() {
		processBacklog.Wait()
		backlogProcessed <- struct{}{}
	}()

	select {
	case <-time.After(backlogProcessTimeout):
		log.Warn("kafka-cluster: Processing metricPersist backlog has taken too long, giving up lock after %s.", backlogProcessTimeout)
	case <-backlogProcessed:
		log.Info("kafka-cluster: metricPersist backlog processed in %s.", time.Since(pre))
	}

}

func (c *NotifierKafka) consumePartition(topic string, partition int32, currentOffset int64, processBacklog *sync.WaitGroup) {
	c.wg.Add(1)
	defer c.wg.Done()

	pc, err := c.consumer.ConsumePartition(topic, partition, currentOffset)
	if err != nil {
		log.Fatal(4, "kafka-cluster: failed to start partitionConsumer for %s:%d. %s", topic, partition, err)
	}
	log.Info("kafka-cluster: consuming from %s:%d from offset %d", topic, partition, currentOffset)

	messages := pc.Messages()
	ticker := time.NewTicker(offsetCommitInterval)
	startingUp := true
	// the bootTimeOffset is the next available offset. There may not be a message with that
	// offset yet, so we subtract 1 to get the highest offset that we can fetch.
	bootTimeOffset := bootTimeOffsets[partition] - 1
	partitionOffsetMetric := partitionOffset[partition]
	partitionLogSizeMetric := partitionLogSize[partition]
	partitionLagMetric := partitionLag[partition]
	for {
		select {
		case msg := <-messages:
			if mdata.LogLevel < 2 {
				log.Debug("kafka-cluster received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
			}
			c.Handle(msg.Value)
			currentOffset = msg.Offset
		case <-ticker.C:
			if err := c.offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-cluster failed to commit offset for %s:%d, %s", topic, partition, err)
			}
			if startingUp && currentOffset >= bootTimeOffset {
				processBacklog.Done()
				startingUp = false
			}
			offset, err := c.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Error(3, "kafka-mdm failed to get log-size of partition %s:%d. %s", topic, partition, err)
			} else {
				partitionLogSizeMetric.Set(int(offset))
			}
			if currentOffset < 0 {
				// we have not yet consumed any messages.
				continue
			}
			partitionOffsetMetric.Set(int(currentOffset))
			if err == nil {
				partitionLagMetric.Set(int(offset - currentOffset))
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
func (c *NotifierKafka) Stop() {
	// closes notifications and messages channels, amongst others
	close(c.stopConsuming)
	c.producer.Close()

	go func() {
		c.wg.Wait()
		c.offsetMgr.Close()
		close(c.StopChan)
	}()
}

func (c *NotifierKafka) Send(sc mdata.SavedChunk) {
	c.in <- sc
}

func (c *NotifierKafka) produce() {
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
func (c *NotifierKafka) flush() {
	if len(c.buf) == 0 {
		return
	}

	// In order to correctly route the saveMessages to the correct partition,
	// we cant send them in batches anymore.
	payload := make([]*sarama.ProducerMessage, 0, len(c.buf))
	var pMsg mdata.PersistMessageBatch
	for i, msg := range c.buf {
		def, ok := c.idx.Get(strings.SplitN(msg.Key, "_", 2)[0])
		if !ok {
			log.Error(3, "kafka-cluster: failed to lookup metricDef with id %s", msg.Key)
			continue
		}
		buf := bytes.NewBuffer(c.bPool.Get())
		binary.Write(buf, binary.LittleEndian, uint8(mdata.PersistMessageBatchV1))
		encoder := json.NewEncoder(buf)
		pMsg = mdata.PersistMessageBatch{Instance: c.instance, SavedChunks: c.buf[i : i+1]}
		err := encoder.Encode(&pMsg)
		if err != nil {
			log.Fatal(4, "kafka-cluster failed to marshal persistMessage to json.")
		}
		messagesSize.Value(buf.Len())
		key := c.bPool.Get()
		key, err = partitioner.GetPartitionKey(&def, key)
		if err != nil {
			log.Fatal(4, "Unable to get partitionKey for metricDef with id %s. %s", def.Id, err)
		}
		kafkaMsg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(buf.Bytes()),
			Key:   sarama.ByteEncoder(key),
		}
		payload = append(payload, kafkaMsg)
	}

	c.buf = nil

	go func() {
		log.Debug("kafka-cluster sending %d batch metricPersist messages", len(payload))
		sent := false
		for !sent {
			err := c.producer.SendMessages(payload)
			if err != nil {
				log.Warn("kafka-cluster publisher %s", err)
			} else {
				sent = true
			}
			time.Sleep(time.Second)
		}
		messagesPublished.Add(len(payload))
		// put our buffers back in the bufferPool
		for _, msg := range payload {
			c.bPool.Put([]byte(msg.Key.(sarama.ByteEncoder)))
			c.bPool.Put([]byte(msg.Value.(sarama.ByteEncoder)))
		}
	}()
}
