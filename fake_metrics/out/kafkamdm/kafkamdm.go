package kafkamdm

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/fake_metrics/out"
	"github.com/raintank/raintank-metric/schema"
)

type KafkaMdm struct {
	out.OutStats
	topic   string
	brokers []string
	config  *sarama.Config
	client  sarama.SyncProducer
}

func New(topic string, brokers []string, stats met.Backend) (*KafkaMdm, error) {
	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaMdm{
		OutStats: out.NewStats(stats, "kafka-mdm"),
		topic:    topic,
		brokers:  brokers,
		config:   config,
		client:   client,
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
		// We are not setting a message key, which means that all messages will
		// be distributed randomly over the different partitions.
		payload[i] = &sarama.ProducerMessage{
			Topic: k.topic,
			Value: sarama.ByteEncoder(data),
		}

	}
	prePub := time.Now()
	err := k.client.SendMessages(payload)
	if err != nil {
		k.PublishErrors.Inc(1)
		return err
	}

	k.PublishedMessages.Inc(int64(len(metrics)))
	k.PublishDuration.Value(time.Since(prePub))
	k.PublishedMetrics.Inc(int64(len(metrics)))
	k.FlushDuration.Value(time.Since(preFlush))
	return nil
}
