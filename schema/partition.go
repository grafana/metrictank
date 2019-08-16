package schema

import (
	"encoding/binary"
	"hash/fnv"
	"sort"

	"github.com/cespare/xxhash"
	jump "github.com/dgryski/go-jump"
)

const prime32 = uint32(16777619)

type PartitionByMethod uint8

const (
	// partition by organization id only
	PartitionByOrg PartitionByMethod = iota

	// partition by the metric name only
	PartitionBySeries

	// partition by metric name and tags, with the best distribution
	// recommended for new deployments.
	PartitionBySeriesWithTags

	// partition by metric name and tags, with a sub-optimal distribution when using tags.
	// compatible with PartitionBySeries if a metric has no tags,
	// making it possible to adopt tags for existing PartitionBySeries deployments without a migration.
	PartitionBySeriesWithTagsFnv
)

func (m *MetricData) PartitionID(method PartitionByMethod, partitions int32) (int32, error) {
	var partition int32

	switch method {
	case PartitionByOrg:
		h := fnv.New32a()
		err := binary.Write(h, binary.LittleEndian, uint32(m.OrgId))
		if err != nil {
			return 0, err
		}
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	case PartitionBySeries:
		h := fnv.New32a()
		h.Write([]byte(m.Name))
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	case PartitionBySeriesWithTags:
		h := xxhash.New()
		if err := writeSortedTagString(h, m.Name, m.Tags); err != nil {
			return 0, err
		}
		partition = jump.Hash(h.Sum64(), int(partitions))
	case PartitionBySeriesWithTagsFnv:
		// this was running too slow so we copied
		// the code from fnv here
		sort.Strings(m.Tags)
		hash := uint32(2166136261)
		for i := 0; i < len(m.Name); i++ {
			hash ^= uint32(m.Name[i])
			hash *= prime32
		}
		for _, t := range m.Tags {
			if len(t) > 5 && t[:5] == "name=" {
				continue
			}
			hash ^= uint32(';')
			hash *= prime32
			for i := 0; i < len(t); i++ {
				hash ^= uint32(t[i])
				hash *= prime32
			}
		}
		partition = int32(hash) % partitions
		if partition < 0 {
			partition = -partition
		}
	default:
		return 0, ErrUnknownPartitionMethod
	}

	return partition, nil
}

func (m *MetricDefinition) PartitionID(method PartitionByMethod, partitions int32) (int32, error) {
	var partition int32

	switch method {
	case PartitionByOrg:
		h := fnv.New32a()
		err := binary.Write(h, binary.LittleEndian, uint32(m.OrgId))
		if err != nil {
			return 0, err
		}
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	case PartitionBySeries:
		h := fnv.New32a()
		h.Write([]byte(m.Name))
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	case PartitionBySeriesWithTags:
		h := xxhash.New()
		h.WriteString(m.NameWithTags())
		partition = jump.Hash(h.Sum64(), int(partitions))
	case PartitionBySeriesWithTagsFnv:
		// this was running too slow so we copied
		// the code from fnv here
		sort.Strings(m.Tags)
		hash := uint32(2166136261)
		if len(m.nameWithTags) > 0 {
			for i := 0; i < len(m.nameWithTags); i++ {
				hash ^= uint32(m.nameWithTags[i])
				hash *= prime32
			}
		} else {
			for i := 0; i < len(m.Name); i++ {
				hash ^= uint32(m.Name[i])
				hash *= prime32
			}
			for _, t := range m.Tags {
				if len(t) > 5 && t[:5] == "name=" {
					continue
				}
				hash ^= uint32(';')
				hash *= prime32
				for i := 0; i < len(t); i++ {
					hash ^= uint32(t[i])
					hash *= prime32
				}
			}
		}
		partition = int32(hash) % partitions
		if partition < 0 {
			partition = -partition
		}
	default:
		return 0, ErrUnknownPartitionMethod
	}

	return partition, nil
}
