package kafkamdm

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	p "github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/stacktest/fakemetrics/out"
	"github.com/raintank/met"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

type KafkaMdm struct {
	out.OutStats
	topic      string
	brokers    []string
	config     *sarama.Config
	client     sarama.SyncProducer
	hash       hash.Hash32
	part       *p.Kafka
	lmPart     LastNumPartitioner
	partScheme string
}

// map the last number in the metricname to the partition
// needless to say, caller beware of how many/which partitions there are
type LastNumPartitioner struct{}

func (p *LastNumPartitioner) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	name := m.KeyBySeries(nil)
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

// key is by metric name, but won't be used for partition setting
func (p *LastNumPartitioner) GetPartitionKey(m schema.PartitionedMetric, b []byte) ([]byte, error) {
	return m.KeyBySeries(b), nil
}

func New(topic string, brokers []string, codec string, stats met.Backend, partitionScheme string) (*KafkaMdm, error) {
	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = out.GetCompression(codec)
	config.Producer.Partitioner = sarama.NewManualPartitioner

	// set all timeouts a bit more aggressive so we can bail out quicker.
	// useful for our unit tests, which operate in the orders of seconds anyway
	// the defaults of 30s is too long for many of our tests.
	config.Net.DialTimeout = 5 * time.Second
	config.Net.ReadTimeout = 5 * time.Second
	config.Net.WriteTimeout = 5 * time.Second
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	var part *p.Kafka
	var lmPart LastNumPartitioner
	switch partitionScheme {
	case "byOrg", "bySeries", "bySeriesWithTags":
		part, err = p.NewKafka(partitionScheme)
	case "lastNum":
		lmPart = LastNumPartitioner{}
		// sets partition based on message partition field
		config.Producer.Partitioner = sarama.NewManualPartitioner
	default:
		err = fmt.Errorf("partitionScheme must be one of 'byOrg|bySeries|bySeriesWithTags|lastNum'. got %s", partitionScheme)
	}
	if err != nil {
		return nil, err
	}

	return &KafkaMdm{
		OutStats:   out.NewStats(stats, "kafka-mdm"),
		topic:      topic,
		brokers:    brokers,
		config:     config,
		client:     client,
		hash:       fnv.New32a(),
		part:       part,
		lmPart:     lmPart,
		partScheme: partitionScheme,
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

		if k.partScheme == "lastNum" {
			part, err := k.lmPart.Partition(metric, 0)
			if err != nil {
				return fmt.Errorf("Failed to get partition for metric. %s", err)
			}

			payload[i] = &sarama.ProducerMessage{
				Partition: part,
				Topic:     k.topic,
				Value:     sarama.ByteEncoder(data),
			}
		} else {
			key, err := k.part.GetPartitionKey(metric, nil)
			if err != nil {
				return fmt.Errorf("Failed to get partition for metric. %s", err)
			}

			payload[i] = &sarama.ProducerMessage{
				Key:   sarama.ByteEncoder(key),
				Topic: k.topic,
				Value: sarama.ByteEncoder(data),
			}
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
