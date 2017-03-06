package partitioner

import (
	"fmt"

	"github.com/Shopify/sarama"
	"gopkg.in/raintank/schema.v1"
)

type Partitioner interface {
	Partition(schema.PartitionedMetric, int32) (int32, error)
}

type Kafka struct {
	PartitionBy string
	Partitioner sarama.Partitioner
}

func NewKafka(partitionBy string) (*Kafka, error) {
	switch partitionBy {
	case "byOrg":
	case "bySeries":
	default:
		return nil, fmt.Errorf("partitionBy must be one of 'byOrg|bySeries'. got %s", partitionBy)
	}
	return &Kafka{
		PartitionBy: partitionBy,
		Partitioner: sarama.NewHashPartitioner(""),
	}, nil
}

func (k *Kafka) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	key, err := k.GetPartitionKey(m, nil)
	if err != nil {
		return 0, err
	}
	return k.Partitioner.Partition(&sarama.ProducerMessage{Key: sarama.ByteEncoder(key)}, numPartitions)
}

func (k *Kafka) GetPartitionKey(m schema.PartitionedMetric, b []byte) ([]byte, error) {
	switch k.PartitionBy {
	case "byOrg":
		// partition by organisation: metrics for the same org should go to the same
		// partition/MetricTank (optimize for locality~performance)
		return m.KeyByOrgId(b), nil
	case "bySeries":
		// partition by series: metrics are distrubted across all metrictank instances
		// to allow horizontal scalability
		return m.KeyBySeries(b), nil
	}
	return b, fmt.Errorf("unknown partitionBy setting.")
}
