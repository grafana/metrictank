package cluster

import (
	"encoding/binary"

	"github.com/raintank/metrictank/kafka/murmur2"
	"gopkg.in/raintank/schema.v1"
)

type IdxPartitioner interface {
	Name() string
	GetPartition(def *schema.MetricDefinition, partitionCount int32) int32
}

type Murmur2Partitioner struct {
}

func (p *Murmur2Partitioner) Name() string {
	return "murmur2"
}

func (p *Murmur2Partitioner) GetPartition(def *schema.MetricDefinition, partitionCount int32) int32 {
	if partitionCount == 1 {
		return 1
	}
	// partition by organisation: metrics for the same org should go to the same
	// partition/MetricTank (optimize for locality~performance)
	// the extra 4B (now initialized with zeroes) is to later enable a smooth transition
	// to a more fine-grained partitioning scheme where
	// large organisations can go to several partitions instead of just one.
	key := make([]byte, 8)
	binary.LittleEndian.PutUint32(key, uint32(def.OrgId))
	h := murmur2.MurmurHash2(key)
	if h < 0 {
		h = -h
	}
	return h % partitionCount
}
