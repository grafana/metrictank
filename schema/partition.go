package schema

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/cespare/xxhash"
	jump "github.com/dgryski/go-jump"
	"github.com/grafana/metrictank/util"
)

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

func PartitonMethodFromString(input string) (PartitionByMethod, error) {
	switch input {
	case "byOrg":
		return PartitionByOrg, nil
	case "bySeries":
		return PartitionBySeries, nil
	case "bySeriesWithTags":
		return PartitionBySeriesWithTags, nil
	case "bySeriesWithTagsFnv":
		return PartitionBySeriesWithTagsFnv, nil
	}
	return 0, fmt.Errorf("partitionBy must be one of 'byOrg|bySeries|bySeriesWithTags|bySeriesWithTagsFnv'. got %s", input)
}

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
		h := util.NewFnv32aStringWriter()
		h.WriteString(m.Name)
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
		h := util.NewFnv32aStringWriter()
		if err := writeSortedTagString(h, m.Name, m.Tags); err != nil {
			return 0, err
		}
		partition = int32(h.Sum32()) % partitions
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
		h := util.NewFnv32aStringWriter()
		h.WriteString(m.Name)
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	case PartitionBySeriesWithTags:
		h := xxhash.New()
		h.WriteString(m.NameWithTags())
		partition = jump.Hash(h.Sum64(), int(partitions))
	case PartitionBySeriesWithTagsFnv:
		h := util.NewFnv32aStringWriter()
		if len(m.nameWithTags) > 0 {
			h.WriteString(m.nameWithTags)
		} else {
			if err := writeSortedTagString(h, m.Name, m.Tags); err != nil {
				return 0, err
			}
		}
		partition = int32(h.Sum32()) % partitions
		if partition < 0 {
			partition = -partition
		}
	default:
		return 0, ErrUnknownPartitionMethod
	}

	return partition, nil
}
