package partitioner

import (
	"fmt"
	"hash"
	"hash/fnv"

	"github.com/cespare/xxhash"
	jump "github.com/dgryski/go-jump"
	"github.com/raintank/schema"
)

type Partitioner interface {
	Key(schema.PartitionedMetric, []byte) []byte
	Partition(schema.PartitionedMetric, int32) (int32, error)
}

// NewKafka returns the appropriate kafka partitioner
// note: for "bySeriesWithTags", do not feed a key obtained from it into a sarama producer
// that uses the default HashPartitioner lest assignments be different!
// for simplicity's sake we recommend you simply construct sarama messages with the partition set
// from the Partition method, which also allows you to not publish any message key.
func NewKafka(partitionBy string) (Partitioner, error) {
	switch partitionBy {
	case "byOrg":
		return KafkaByOrg{fnv.New32a()}, nil
	case "bySeries":
		return KafkaBySeries{fnv.New32a()}, nil
	case "bySeriesWithTags":
		return KafkaBySeriesWithTags{}, nil
	}
	return nil, fmt.Errorf("partitionBy must be one of 'byOrg|bySeries|bySeriesWithTags'. got %s", partitionBy)
}

// KafkaByOrg partitions a schema.PartitionedMetric by OrgId, using hashing equivalent to sarama.HashPartitioner
type KafkaByOrg struct {
	hasher hash.Hash32
}

func (k KafkaByOrg) Key(m schema.PartitionedMetric, b []byte) []byte {
	return m.KeyByOrgId(b)
}

// Partition partitions just like sarama.HashPartitioner but without needing a *sarama.ProducerMessage
func (k KafkaByOrg) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	k.hasher.Reset()
	_, err := k.hasher.Write(k.Key(m, nil))
	if err != nil {
		return -1, err
	}
	partition := int32(k.hasher.Sum32()) % numPartitions
	if partition < 0 {
		partition = -partition
	}
	return partition, nil
}

// KafkaBySeries partitions a schema.PartitionedMetric by Series name, using hashing equivalent to sarama.HashPartitioner
type KafkaBySeries struct {
	hasher hash.Hash32
}

func (k KafkaBySeries) Key(m schema.PartitionedMetric, b []byte) []byte {
	return m.KeyBySeries(b)
}

// Partition partitions just like sarama.HashPartitioner but without needing a *sarama.ProducerMessage
func (k KafkaBySeries) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	k.hasher.Reset()
	_, err := k.hasher.Write(k.Key(m, nil))
	if err != nil {
		return -1, err
	}
	partition := int32(k.hasher.Sum32()) % numPartitions
	if partition < 0 {
		partition = -partition
	}
	return partition, nil
}

// KafkaBySeriesWithTags partitions a schema.PartitionedMetric by nameWithTags, using a custom xxhash+jump hashing scheme
// DO NOT feed a key obtained from this into a sarama producer that uses the default HashPartitioner lest assignments be different!
type KafkaBySeriesWithTags struct{}

func (k KafkaBySeriesWithTags) Key(m schema.PartitionedMetric, b []byte) []byte {
	return m.KeyBySeriesWithTags(b)
}

// Partition partitions using a custom xxhash+jump hashing scheme
func (k KafkaBySeriesWithTags) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	jumpKey := xxhash.Sum64(k.Key(m, nil))
	return jump.Hash(jumpKey, int(numPartitions)), nil
}
