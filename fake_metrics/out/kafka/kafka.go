package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/fake_metrics/out/lib"
	"github.com/raintank/raintank-metric/msg"
	"github.com/raintank/raintank-metric/schema"
)

var (
	metricsPublished  met.Count
	messagesPublished met.Count
	messagesSize      met.Meter
	metricsPerMessage met.Meter
	publishDuration   met.Timer
)

type Kafka struct {
	topic   string
	brokers []string
	config  *sarama.Config
	client  sarama.SyncProducer
}

func New(topic string, brokers []string, stats met.Backend) (*Kafka, error) {
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

	metricsPublished = stats.NewCount("metricpublisher.kafka.metrics-published")
	messagesPublished = stats.NewCount("metricpublisher.kafka.messages-published")
	messagesSize = stats.NewMeter("metricpublisher.kafka.message_size", 0)
	metricsPerMessage = stats.NewMeter("metricpublisher.kafka.metrics_per_message", 0)
	publishDuration = stats.NewTimer("metricpublisher.kafka.publish_duration", 0)

	return &Kafka{
		topic:   topic,
		brokers: brokers,
		config:  config,
		client:  client,
	}, nil
}

func (k *Kafka) Close() error {
	return k.client.Close()
}

func (k *Kafka) Publish(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}
	// typical metrics seem to be around 300B
	// nsqd allows <= 10MiB messages.
	// we ideally have 64kB ~ 1MiB messages (see benchmark https://gist.github.com/Dieterbe/604232d35494eae73f15)
	// at 300B, about 3500 msg fit in 1MiB
	// in worst case, this allows messages up to 2871B
	// this could be made more robust of course

	// real world findings in dev-stack with env-load:
	// 159569B msg /795  metrics per msg = 200B per msg
	// so peak message size is about 3500*200 = 700k (seen 711k)

	subslices := lib.Reslice(metrics, 3500)

	for _, subslice := range subslices {
		id := time.Now().UnixNano()
		data, err := msg.CreateMsg(subslice, id, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			return err
		}

		messagesSize.Value(int64(len(data)))
		metricsPerMessage.Value(int64(len(subslice)))

		pre := time.Now()

		// We are not setting a message key, which means that all messages will
		// be distributed randomly over the different partitions.
		_, _, err = k.client.SendMessage(&sarama.ProducerMessage{
			Topic: k.topic,
			Value: sarama.ByteEncoder(data),
		})
		if err != nil {
			return err
		}

		publishDuration.Value(time.Since(pre))
		metricsPublished.Inc(int64(len(subslice)))
		messagesPublished.Inc(1)

		if err != nil {
			return err
		}
	}
	return nil
}
