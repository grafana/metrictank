package partitioner

import (
	"fmt"

	"github.com/raintank/schema"
)

type Partitioner interface {
	Partition(schema.PartitionedMetric, int32) (int32, error)
}

type Kafka struct {
	PartitionBy string
}

func NewKafka(partitionBy string) (*Kafka, error) {
	switch partitionBy {
	case "byOrg":
	case "bySeries":
	case "bySeriesWithTags":
	default:
		return nil, fmt.Errorf("partitionBy must be one of 'byOrg|bySeries|bySeriesWithTags'. got %s", partitionBy)
	}
	return &Kafka{
		PartitionBy: partitionBy,
	}, nil
}

func (k *Kafka) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	partition, err := k.GetPartition(m, numPartitions)
	if err != nil {
		return 0, err
	}
	return partition, nil
}

func (k *Kafka) GetPartition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	switch k.PartitionBy {
	case "byOrg":
		// partition by organisation: metrics for the same org should go to the same
		// partition/MetricTank (optimize for locality~performance)
		return m.PartitionID(schema.PartitionByOrg, numPartitions)
	case "bySeries":
		// partition by series: metrics are distributed across all metrictank instances
		// to allow horizontal scalability
		return m.PartitionID(schema.PartitionBySeries, numPartitions)
	case "bySeriesWithTags":
		// partition by series with tags: metrics are distributed across all metrictank instances
		// to allow horizontal scalability
		return m.PartitionID(schema.PartitionBySeriesWithTags, numPartitions)
	}
	return -1, fmt.Errorf("unknown partitionBy setting.")
}
