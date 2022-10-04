package partitioner

import (
	"github.com/grafana/metrictank/schema"
)

type Partitioner interface {
	Partition(schema.PartitionedMetric, int32) (int32, error)
}

type Kafka struct {
	Method schema.PartitionByMethod
}

func NewKafka(partitionBy string) (*Kafka, error) {
	method, err := schema.PartitonMethodFromString(partitionBy)
	if err != nil {
		return nil, err
	}
	return &Kafka{Method: method}, err
}

func (k *Kafka) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	return m.PartitionID(k.Method, numPartitions)
}
