package kafkamdm

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	p "github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stacktest/fakemetrics/out"
	"github.com/raintank/met"
	log "github.com/sirupsen/logrus"
)

type KafkaMdm struct {
	out.OutStats
	topic         string
	brokers       []string
	config        *sarama.Config
	client        sarama.SyncProducer
	part          p.Partitioner
	numPartitions int32
}

// map the last number in the metricname to the partition
// needless to say, caller beware of how many/which partitions there are
type LastNumPartitioner struct{}

func (p *LastNumPartitioner) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	name := []byte(m.(*schema.MetricData).Name)
	index := bytes.LastIndexByte(name, '.')
	if index < 0 || index == len(name)-1 {
		return 0, fmt.Errorf("invalid metricname for LastNumPartitioner: '%s'", name)
	}
	part, err := strconv.ParseInt(string(name[index+1:]), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid metricname for LastNumPartitioner: '%s'", name)
	}
	return int32(part), nil
}

func New(topic string, brokers []string, codec string, timeout time.Duration, stats met.Backend, partitionScheme string) (*KafkaMdm, error) {
	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = out.GetCompression(codec)
	config.Producer.Partitioner = sarama.NewManualPartitioner

	config.Net.DialTimeout = timeout
	config.Net.ReadTimeout = timeout
	config.Net.WriteTimeout = timeout
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}
	if len(partitions) < 1 {
		return nil, fmt.Errorf("failed to get number of partitions for topic: %s", topic)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	var part p.Partitioner
	if partitionScheme == "lastNum" {
		part = &LastNumPartitioner{}
	} else {
		part, err = p.NewKafka(partitionScheme)
		if err != nil {
			return nil, fmt.Errorf("partitionScheme must be one of 'byOrg|bySeries|bySeriesWithTags|lastNum'. got %s", partitionScheme)
		}
	}

	return &KafkaMdm{
		OutStats:      out.NewStats(stats, "kafka-mdm"),
		topic:         topic,
		brokers:       brokers,
		config:        config,
		client:        producer,
		part:          part,
		numPartitions: int32(len(partitions)),
	}, nil
}

func (k *KafkaMdm) Close() error {
	return k.client.Close()
}

func (k *KafkaMdm) Flush(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		k.FlushDuration.Value(0)
		return nil
	}
	preFlush := time.Now()

	k.MessageMetrics.Value(1)
	var data []byte

	payload := make([]*sarama.ProducerMessage, len(metrics))

	for i, metric := range metrics {
		data, err := metric.MarshalMsg(data[:])
		if err != nil {
			return err
		}

		k.MessageBytes.Value(int64(len(data)))

		partition, err := k.part.Partition(metric, k.numPartitions)
		if err != nil {
			return fmt.Errorf("Failed to get partition for metric. %s", err)
		}

		payload[i] = &sarama.ProducerMessage{
			Partition: partition,
			Topic:     k.topic,
			Value:     sarama.ByteEncoder(data),
		}

	}
	prePub := time.Now()
	err := k.client.SendMessages(payload)
	if err != nil {
		k.PublishErrors.Inc(1)
		if errors, ok := err.(sarama.ProducerErrors); ok {
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Errorf("ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
			}
		}
		return err
	}

	k.PublishedMessages.Inc(int64(len(metrics)))
	k.PublishDuration.Value(time.Since(prePub))
	k.PublishedMetrics.Inc(int64(len(metrics)))
	k.FlushDuration.Value(time.Since(preFlush))
	return nil
}
