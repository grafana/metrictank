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
	Offset   Gauge64
	LogSize  Gauge64
	Lag      Gauge64
	Priority Gauge64
	Ready    Bool
}

func NewKafkaPartition(prefix string) *KafkaPartition {
	k := KafkaPartition{}
	registry.getOrAdd(prefix+".offset", &k.Offset)
	registry.getOrAdd(prefix+".log_size", &k.LogSize)
	registry.getOrAdd(prefix+".lag", &k.Lag)
	registry.getOrAdd(prefix+".priority", &k.Priority)
	registry.getOrAdd(prefix+".ready", &k.Ready)
	return &k
}
