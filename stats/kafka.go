package stats

import "strconv"

// Kafka tracks the health of a consumer
type Kafka map[int32]*KafkaPartition

func NewKafka(prefix string, partitions []int32) Kafka {
	k := make(map[int32]*KafkaPartition)
	for _, part := range partitions {
		k[part] = NewKafkaPartition(prefix + ".partition." + strconv.Itoa(int(part)))
	}
	return k
}

// KafkaPartition tracks the health of a partition consumer
type KafkaPartition struct {
	Offset   *Gauge64 // offset of consumed message
	LogSize  *Gauge64 // last offset in the topic
	Lag      *Gauge64 // difference between the two above
	Priority *Gauge64
	Ready    *Bool
}

func NewKafkaPartition(prefix string) *KafkaPartition {
	k := KafkaPartition{}
	k.Offset = NewGauge64(prefix + ".offset")
	k.LogSize = NewGauge64(prefix + ".log_size")
	k.Lag = NewGauge64(prefix + ".lag")
	k.Priority = NewGauge64(prefix + ".priority")
	k.Ready = NewBool(prefix + ".ready")
	return &k
}
