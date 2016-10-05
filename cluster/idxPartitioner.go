package cluster

import (
	"encoding/binary"
	"hash"
	"hash/fnv"

	"github.com/raintank/metrictank/kafka/murmur2"
	"gopkg.in/raintank/schema.v1"
)

type IdxPartitioner interface {
	Name() string
	GetPartition(def *schema.MetricDefinition, partitionCount int32) int32
}

func KeyFromDef(def *schema.MetricDefinition) []byte {
	// partition by organisation: metrics for the same org should go to the same
	// partition/MetricTank (optimize for locality~performance)
	// the extra 4B (now initialized with zeroes) is to later enable a smooth transition
	// to a more fine-grained partitioning scheme where
	// large organisations can go to several partitions instead of just one.
	key := make([]byte, 8)
	binary.LittleEndian.PutUint32(key, uint32(def.OrgId))
	return key
}

type Murmur2Partitioner struct {
}

func NewMurmur2Partitioner() *Murmur2Partitioner {
	return &Murmur2Partitioner{}
}

func (p *Murmur2Partitioner) Name() string {
	return "murmur2"
}

func (p *Murmur2Partitioner) GetPartition(def *schema.MetricDefinition, partitionCount int32) int32 {
	if partitionCount == 1 {
		return 1
	}
	key := KeyFromDef(def)
	h := murmur2.MurmurHash2(key)
	if h < 0 {
		h = -h
	}
	return h % partitionCount
}

type FNV1aPartitioner struct {
	hasher hash.Hash32
}

func (p *FNV1aPartitioner) Name() string {
	return "fnv1a"
}

func NewFNV1aPartitioner() *FNV1aPartitioner {
	return &FNV1aPartitioner{
		hasher: fnv.New32a(),
	}
}

func (p *FNV1aPartitioner) GetPartition(def *schema.MetricDefinition, partitionCount int32) int32 {
	if partitionCount == 1 {
		return 1
	}
	key := KeyFromDef(def)
	p.hasher.Reset()
	p.hasher.Write(key)
	partition := int32(p.hasher.Sum32()) % partitionCount
	if partition < 0 {
		partition = -partition
	}
	return partition
}
