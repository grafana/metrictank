package main

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/grafana/metrictank/cluster/partitioner"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

func GetCompression(codec string) sarama.CompressionCodec {
	switch codec {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	default:
		log.Fatal(5, "unknown compression codec %q", codec)
		return 0 // make go compiler happy, needs a return *roll eyes*
	}
}

type MetricsReplicator struct {
	consumer    *cluster.Consumer
	producer    sarama.SyncProducer
	partitioner *partitioner.Kafka
	destTopic   string
	done        chan struct{}
}

func NewMetricsReplicator(srcBrokers, dstBrokers []string, compression, group, srcTopic, destTopic string, initialOffset int, partitionScheme string) (*MetricsReplicator, error) {
	config := cluster.NewConfig()
	config.Consumer.Offsets.Initial = int64(initialOffset)
	config.ClientID = "mt-replicator"
	config.Group.Return.Notifications = true
	config.ChannelBufferSize = 1000
	config.Consumer.Fetch.Min = 1
	config.Consumer.Fetch.Default = 32768
	config.Consumer.MaxWaitTime = time.Second
	config.Consumer.MaxProcessingTime = time.Second
	config.Config.Version = sarama.V0_10_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = GetCompression(compression)
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	consumer, err := cluster.NewConsumer(srcBrokers, group, []string{srcTopic}, config)
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewSyncProducer(dstBrokers, &config.Config)
	if err != nil {
		return nil, err
	}
	partitioner, err := partitioner.NewKafka(partitionScheme)
	if err != nil {
		return nil, err
	}

	return &MetricsReplicator{
		consumer:    consumer,
		producer:    producer,
		partitioner: partitioner,
		destTopic:   destTopic,
		done:        make(chan struct{}),
	}, nil
}

func (r *MetricsReplicator) Consume() {
	buf := make([]*schema.MetricData, 0)
	accountingTicker := time.NewTicker(time.Second * 10)
	flushTicker := time.NewTicker(time.Second)
	counter := 0
	counterTs := time.Now()
	msgChan := r.consumer.Messages()
	var m *sarama.ConsumerMessage
	var ok bool

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
			md := &schema.MetricData{}
			_, err := md.UnmarshalMsg(m.Value)
			if err != nil {
				log.Error(3, "kafka-mdm decode error, skipping message. %s", err)
				continue
			}

			buf = append(buf, md)
			if len(buf) > 1000 {
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
			log.Info("%d metrics processed in last %.1fseconds.", counter, t.Sub(counterTs).Seconds())
			counter = 0
			counterTs = t
		}
	}

}

func (r *MetricsReplicator) Stop() {
	r.consumer.Close()
	<-r.done
	r.producer.Close()
}

func (r *MetricsReplicator) Start() {
	go r.Consume()
}

func (r *MetricsReplicator) Flush(metrics []*schema.MetricData) {
	payload := make([]*sarama.ProducerMessage, len(metrics))

	for i, metric := range metrics {
		data, err := metric.MarshalMsg(nil)
		if err != nil {
			log.Fatal(4, "Failed to Marshal metric. %s", err)
		}

		key, err := r.partitioner.GetPartitionKey(metric, nil)
		if err != nil {
			log.Fatal(4, "Failed to get partitionKey for metric. %s", err)
		}

		payload[i] = &sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(key),
			Topic: r.destTopic,
			Value: sarama.ByteEncoder(data),
		}

	}
	for {
		err := r.producer.SendMessages(payload)
		if err != nil {
			if errors, ok := err.(sarama.ProducerErrors); ok {
				for i := 0; i < 10 && i < len(errors); i++ {
					log.Error(4, "ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
				}
			} else {
				log.Error(4, "ProducerError %s", err)
			}
		} else {
			return
		}
	}
}
