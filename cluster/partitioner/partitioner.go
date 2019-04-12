package partitioner

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/Shopify/sarama"
	"github.com/cespare/xxhash"
	"github.com/dchest/siphash"
	jump "github.com/dgryski/go-jump"
	metro "github.com/dgryski/go-metro"
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
	kafka := Kafka{
		PartitionBy: partitionBy,
	}

	switch partitionBy {
	case "byOrg":
		kafka.Partitioner = sarama.NewHashPartitioner("")
		kafka.GetPartitionKey = func(m schema.PartitionedMetric, b []byte) []byte { return m.KeyByOrgId(b) }
	case "bySeries":
		kafka.Partitioner = sarama.NewHashPartitioner("")
		kafka.GetPartitionKey = func(m schema.PartitionedMetric, b []byte) []byte { return m.KeyBySeries(b) }
	case "bySeriesWithTags":
		kafka.Partitioner = sarama.NewHashPartitioner("")
		kafka.GetPartitionKey = func(m schema.PartitionedMetric, b []byte) []byte { return m.KeyBySeriesWithTags(b) }
	default:
		return nil, fmt.Errorf("partitionBy must be one of 'byOrg|bySeries|bySeriesWithTags'. got %s", partitionBy)
	}

	return &kafka, nil
}

func (k *Kafka) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	key := k.GetPartitionKey(m, nil)
	return k.Partitioner.Partition(&sarama.ProducerMessage{Key: sarama.ByteEncoder(key)}, numPartitions)
}

type jumpPartitionerMauro struct{}

func (p jumpPartitionerMauro) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
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

func (p jumpPartitionerMauro) RequiresConsistency() bool {
	return true
}

type jumpPartitionerFnv struct{}

func (p jumpPartitionerFnv) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	key, err := message.Key.Encode()
	if err != nil {
		return 0, err
	}
	hf := fnv.New64a()
	_, _ = hf.Write(key)
	return jump.Hash(hf.Sum64(), int(numPartitions)), nil
}

func (p jumpPartitionerFnv) RequiresConsistency() bool {
	return true
}

type jumpPartitionerMetro struct{}

func (p jumpPartitionerMetro) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	key, err := message.Key.Encode()
	if err != nil {
		return 0, err
	}
	jumpKey := metro.Hash64(key, 0)
	return jump.Hash(jumpKey, int(numPartitions)), nil

}

func (p jumpPartitionerMetro) RequiresConsistency() bool {
	return true
}

type jumpPartitionerSip struct{}

func (p jumpPartitionerSip) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	key, err := message.Key.Encode()
	if err != nil {
		return 0, err
	}
	jumpKey := siphash.Hash(0, 0, key)
	return jump.Hash(jumpKey, int(numPartitions)), nil

}

func (p jumpPartitionerSip) RequiresConsistency() bool {
	return true
}

type jumpPartitionerXxhash struct{}

func (p jumpPartitionerXxhash) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	key, err := message.Key.Encode()
	if err != nil {
		return 0, err
	}
	jumpKey := xxhash.Sum64(key)
	return jump.Hash(jumpKey, int(numPartitions)), nil

}

func (p jumpPartitionerXxhash) RequiresConsistency() bool {
	return true
}
