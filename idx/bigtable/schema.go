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

// FormatRowKey formats an MKey and partition into a rowKey
func FormatRowKey(mkey schema.MKey, partition int32) string {
	return strconv.Itoa(int(partition)) + "_" + mkey.String()
}

// SchemaToRow takes a metricDefintion and returns a rowKey and column data.
func SchemaToRow(def *idx.MetricDefinition) (string, map[string][]byte) {
	row := map[string][]byte{
		//"Id" omitted as it is part of the rowKey
		"OrgId":      make([]byte, 8),
		"Name":       []byte(def.Name.String()),
		"Interval":   make([]byte, 8),
		"Unit":       []byte(def.Unit),
		"Mtype":      []byte(def.Mtype()),
		"Tags":       []byte(strings.Join(def.Tags.Strings(), ";")),
		"LastUpdate": make([]byte, 8),
		//"Partition" omitted as it is part of te rowKey
	}
	binary.PutVarint(row["OrgId"], int64(def.OrgId))
	binary.PutVarint(row["Interval"], int64(def.Interval))
	binary.PutVarint(row["LastUpdate"], def.LastUpdate)
	return FormatRowKey(def.Id, def.Partition), row
}

// DecodeRowKey takes a rowKey string and returns the corresponding MKey and partition
func DecodeRowKey(key string) (schema.MKey, int32, error) {
	parts := strings.SplitN(key, "_", 2)
	partition, err := strconv.Atoi(parts[0])
	if err != nil {
		return schema.MKey{}, 0, err
	}
	mkey, err := schema.MKeyFromString(parts[1])
	if err != nil {
		return schema.MKey{}, 0, err
	}
	return mkey, int32(partition), nil
}

// RowToSchema takes a row and unmarshals the data into the provided MetricDefinition.
func RowToSchema(row bigtable.Row, def *idx.MetricDefinition) error {
	if def == nil {
		return fmt.Errorf("cant write row to nil MetricDefinition")
	}
	columns, ok := row[COLUMN_FAMILY]
	if !ok {
		return fmt.Errorf("no columns in columnFamly %s", COLUMN_FAMILY)
	}
	*def = idx.MetricDefinition{}
	var err error
	var val int64

	def.Id, def.Partition, err = DecodeRowKey(row.Key())
	if err != nil {
		return err
	}
	for _, col := range columns {
		switch strings.SplitN(col.Column, ":", 2)[1] {
		case "OrgId":
			val, err = binary.ReadVarint(bytes.NewReader(col.Value))
			if err != nil {
				return err
			}
			if val < 0 {
				def.OrgId = idx.OrgIdPublic
			} else {
				def.OrgId = uint32(val)
			}
		case "Name":
			def.SetMetricName(string(col.Value))
		case "Interval":
			val, err = binary.ReadVarint(bytes.NewReader(col.Value))
			if err != nil {
				return err
			}
			def.Interval = int(val)
		case "Unit":
			def.SetUnit(string(col.Value))
		case "Mtype":
			def.SetMType(string(col.Value))
		case "Tags":
			if len(col.Value) == 0 {
				def.Tags.KeyValues = nil
			} else {
				def.SetTags(strings.Split(string(col.Value), ";"))
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
