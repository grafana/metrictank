package kafkamdam

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/met"
)

// kafka output that sends MetricDataArrayMsgp messages
type KafkaMdam struct {
	out.OutStats
	topic   string
	brokers []string
	config  *sarama.Config
	client  sarama.SyncProducer
}

func New(topic string, brokers []string, codec string, stats met.Backend) (*KafkaMdam, error) {
	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = out.GetCompression(codec)
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaMdam{
		OutStats: out.NewStats(stats, "kafka-mdam"),
		topic:    topic,
		brokers:  brokers,
		config:   config,
		client:   client,
	}, nil
}

func (k *KafkaMdam) Close() error {
	return k.client.Close()
}

func (k *KafkaMdam) Flush(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		k.FlushDuration.Value(0)
		return nil
	}
	preFlush := time.Now()
	// typical metrics seem to be around 300B
	// we ideally have 64kB ~ 1MiB messages (see benchmark https://gist.github.com/Dieterbe/604232d35494eae73f15)
	// at 300B, about 3500 msg fit in 1MiB
	// in worst case, this allows messages up to 2871B
	// this could be made more robust of course

	// real world findings in dev-stack with env-load:
	// 159569B msg /795  metrics per msg = 200B per msg
	// so peak message size is about 3500*200 = 700k (seen 711k)

	subslices := schema.Reslice(metrics, 3500)

	for _, subslice := range subslices {
		id := time.Now().UnixNano()
		data, err := msg.CreateMsg(subslice, id, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			return err
		}

		k.MessageBytes.Value(int64(len(data)))
		k.MessageMetrics.Value(int64(len(subslice)))

		prePub := time.Now()

		// We cannot set a message key, because metrics may have different orgs and other properties,
		// which means that all messages will be distributed randomly over the different partitions.
		_, _, err = k.client.SendMessage(&sarama.ProducerMessage{
			Topic: k.topic,
			Value: sarama.ByteEncoder(data),
		})
		if err != nil {
			k.PublishErrors.Inc(1)
			return err
		}

		k.PublishedMetrics.Inc(int64(len(subslice)))
		k.PublishedMessages.Inc(1)
		k.PublishDuration.Value(time.Since(prePub))
	}
	k.FlushDuration.Value(time.Since(preFlush))
	return nil
}
