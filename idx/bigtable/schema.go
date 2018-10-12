package bigtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/grafana/metrictank/idx"
	"github.com/raintank/schema"
)

func SchemaToRow(def *schema.MetricDefinition) map[string][]byte {
	row := map[string][]byte{
		"Id":         []byte(def.Id.String()),
		"OrgId":      make([]byte, 8),
		"Name":       []byte(def.Name),
		"Interval":   make([]byte, 8),
		"Unit":       []byte(def.Unit),
		"Mtype":      []byte(def.Mtype),
		"Tags":       []byte(strings.Join(def.Tags, ";")),
		"LastUpdate": make([]byte, 8),
		"Partition":  make([]byte, 8),
	}
	binary.PutVarint(row["OrgId"], int64(def.OrgId))
	binary.PutVarint(row["Interval"], int64(def.Interval))
	binary.PutVarint(row["LastUpdate"], def.LastUpdate)
	binary.PutVarint(row["Partition"], int64(def.Partition))
	return row
}

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
	for _, col := range columns {
		switch strings.SplitN(col.Column, ":", 2)[1] {
		case "Id":
			mkey, err := schema.MKeyFromString(string(col.Value))
			if err != nil {
				return err
			}
			def.Id = mkey
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
		case "Partition":
			val, err = binary.ReadVarint(bytes.NewReader(col.Value))
			if err != nil {
				return err
			}
			def.Partition = int32(val)
		default:
			return fmt.Errorf("unknown column: %s", col.Column)
		}
	}
	return nil
}
