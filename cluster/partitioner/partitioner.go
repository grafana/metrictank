package partitioner

import (
	"fmt"

	"github.com/raintank/schema"
)

type Partitioner interface {
	Partition(schema.PartitionedMetric, int32) (int32, error)
}

type Kafka struct {
	Method schema.PartitionByMethod
}

func NewKafka(partitionBy string) (*Kafka, error) {
	var method schema.PartitionByMethod
	switch partitionBy {
	case "byOrg":
		method = schema.PartitionByOrg
	case "bySeries":
		method = schema.PartitionBySeries
	case "bySeriesWithTags":
		method = schema.PartitionBySeriesWithTags
	default:
		return nil, fmt.Errorf("partitionBy must be one of 'byOrg|bySeries|bySeriesWithTags'. got %s", partitionBy)
	}
	return &Kafka{Method: method}, nil
}

func (k *Kafka) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	return m.PartitionID(k.Method, numPartitions)
}
