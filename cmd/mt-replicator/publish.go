package main

import (
	"github.com/Shopify/sarama"
	part "github.com/raintank/metrictank/cluster/partitioner"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type Publisher struct {
	topic       string
	producer    sarama.SyncProducer
	partitioner *part.Kafka
}

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

func NewPublisher(brokers []string, topic string, compression string, partitionScheme string) (*Publisher, error) {
	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = GetCompression(compression)
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	partitioner, err := part.NewKafka(partitionScheme)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		topic:       topic,
		producer:    client,
		partitioner: partitioner,
	}, nil
}

func (p *Publisher) Stop() error {
	return p.producer.Close()
}

func (p *Publisher) Send(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}

	payload := make([]*sarama.ProducerMessage, len(metrics))

	for i, metric := range metrics {
		data, err := metric.MarshalMsg(nil)
		if err != nil {
			return err
		}

		key, err := p.partitioner.GetPartitionKey(metric, nil)
		if err != nil {
			return err
		}

		payload[i] = &sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(key),
			Topic: p.topic,
			Value: sarama.ByteEncoder(data),
		}

	}
	err := p.producer.SendMessages(payload)
	if err != nil {
		if errors, ok := err.(sarama.ProducerErrors); ok {
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Error(4, "ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
			}
		}
		return err
	}
	return nil
}
