package bigtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/grafana/metrictank/idx"
	"github.com/raintank/schema"
)

// FormatRowKey takes an id and partition and returns a rowKey
func FormatRowKey(id string, partition int32) string {
	return strconv.Itoa(int(partition)) + "_" + id
}

// SchemaToRow takes a metricDefintion and returns a rowKey and column data.
func SchemaToRow(def *schema.MetricDefinition) (string, map[string][]byte) {
	row := map[string][]byte{
		//"Id" omitted as it is part of the rowKey
		"OrgId":      make([]byte, 8),
		"Name":       []byte(def.Name),
		"Interval":   make([]byte, 8),
		"Unit":       []byte(def.Unit),
		"Mtype":      []byte(def.Mtype),
		"Tags":       []byte(strings.Join(def.Tags, ";")),
		"LastUpdate": make([]byte, 8),
		//"Partition" omitted as it is part of te rowKey
	}
	binary.PutVarint(row["OrgId"], int64(def.OrgId))
	binary.PutVarint(row["Interval"], int64(def.Interval))
	binary.PutVarint(row["LastUpdate"], def.LastUpdate)
	return FormatRowKey(def.Id.String(), def.Partition), row
}

// DecodeRowKey takes a rowKey string and returns the id and partition it contains
func DecodeRowKey(key string) (string, int32, error) {
	parts := strings.SplitN(key, "_", 2)
	partition, err := strconv.Atoi(parts[0])
	if err != nil {
		return "", 0, err
	}
	return parts[1], int32(partition), nil
}

// RowToSchema takes a row and unmarshals the data into the provided MetricDefinition.
func RowToSchema(row bigtable.Row, def *schema.MetricDefinition) error {
	if def == nil {
		return fmt.Errorf("cant write row to nill MetricDefinition")
	}
	columns, ok := row[COLUMN_FAMILY]
	if !ok {
		return fmt.Errorf("no columns in columnFamly %s", COLUMN_FAMILY)
	}
	*def = schema.MetricDefinition{}
	var err error
	var val int64

	id, partition, err := DecodeRowKey(row.Key())
	if err != nil {
		return err
	}
	mkey, err := schema.MKeyFromString(id)
	if err != nil {
		return err
	}
	def.Id = mkey
	def.Partition = partition
	for _, col := range columns {
		switch strings.SplitN(col.Column, ":", 2)[1] {
		case "OrgId":
			val, err = binary.ReadVarint(bytes.NewReader(col.Value))
			if err != nil {
				return err
			}
			def.OrgId = uint32(val)
			if def.OrgId < 0 {
				def.OrgId = idx.OrgIdPublic
			}
		case "Name":
			def.Name = string(col.Value)
		case "Interval":
			val, err = binary.ReadVarint(bytes.NewReader(col.Value))
			if err != nil {
				return err
			}
			def.Interval = int(val)
		case "Unit":
			def.Unit = string(col.Value)
		case "Mtype":
			def.Mtype = string(col.Value)
		case "Tags":
			if len(col.Value) == 0 {
				def.Tags = nil
			} else {
				def.Tags = strings.Split(string(col.Value), ";")
			}
		case "LastUpdate":
			def.LastUpdate, err = binary.ReadVarint(bytes.NewReader(col.Value))
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown column: %s", col.Column)
		}
	}
	return nil
}
