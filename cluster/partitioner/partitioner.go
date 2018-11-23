package partitioner

import (
	"encoding/binary"
	"fmt"

	"github.com/Shopify/sarama"
	jump "github.com/dgryski/go-jump"
	"github.com/raintank/schema"
)

type Partitioner interface {
	Partition(schema.PartitionedMetric, int32) (int32, error)
}

type Kafka struct {
	PartitionBy     string
	Partitioner     sarama.Partitioner
	GetPartitionKey func(schema.PartitionedMetric, []byte) []byte
}

func NewKafka(partitionBy string) (*Kafka, error) {
	switch partitionBy {
	case "byOrg":
		return &Kafka{
			PartitionBy:     partitionBy,
			Partitioner:     sarama.NewHashPartitioner(""),
			GetPartitionKey: func(m schema.PartitionedMetric, b []byte) []byte { return m.KeyByOrgId(b) },
		}, nil
	case "bySeries":
		return &Kafka{
			PartitionBy:     partitionBy,
			Partitioner:     sarama.NewHashPartitioner(""),
			GetPartitionKey: func(m schema.PartitionedMetric, b []byte) []byte { return m.KeyBySeries(b) },
		}, nil
	case "bySeriesWithTags":
		return &Kafka{
			PartitionBy:     partitionBy,
			Partitioner:     &jumpPartitioner{},
			GetPartitionKey: func(m schema.PartitionedMetric, b []byte) []byte { return m.KeyBySeriesWithTags(b) },
		}, nil
	default:
		return nil, fmt.Errorf("partitionBy must be one of 'byOrg|bySeries|bySeriesWithTags'. got %s", partitionBy)
	}
}

func (k *Kafka) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	key := k.GetPartitionKey(m, nil)
	return k.Partitioner.Partition(&sarama.ProducerMessage{Key: sarama.ByteEncoder(key)}, numPartitions)
}

type jumpPartitioner struct{}

func (p *jumpPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	key, err := message.Key.Encode()
	if err != nil {
		return 0, err
	}

	// we need to get from a []byte key to a uint64 key, because jump requires that as input
	// so we keep adding slices of 8 bytes to the jump key as uint64 values
	// if the result wraps around the uin64 boundary it doesn't matter because
	// this only needs to be fast and consistent
	key = append(key, make([]byte, 8-len(key)%8)...)
	var jumpKey uint64
	for pos := 8; pos <= len(key); pos += 8 {
		jumpKey += binary.BigEndian.Uint64(key[pos-8 : pos])
	}

	return jump.Hash(jumpKey, int(numPartitions)), nil
}

func (p *jumpPartitioner) RequiresConsistency() bool {
	return true
}
