package out

import (
	"fmt"
	"testing"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/idx"
)

func genTags(num int) []string {
	tags := make([]string, num)
	for i := 0; i < num; i++ {
		tags[i] = fmt.Sprintf("key%d=val%d", i, i)
	}
	return tags
}

func genMetricDefinitions(num int) []idx.MetricDefinition {
	defs := make([]idx.MetricDefinition, num)
	for i := 0; i < num; i++ {
		defs[i] = idx.MetricDefinition{
			OrgId:      1,
			Interval:   10,
			LastUpdate: 0,
			Partition:  1,
		}
		defs[i].SetTags(genTags(5))
		defs[i].SetMType("rate")
		defs[i].SetMetricName(fmt.Sprintf("anotheryetlonger.short.metric.name%d", i))
		defs[i].SetUnit("test")
		defs[i].SetId()
	}
	return defs
}

func compareSchemaWithIdxMd(smd schema.MetricDefinition, imd idx.MetricDefinition) error {
	if smd.Id != imd.Id {
		return fmt.Errorf("Id not equal. schema: %d, idx: %d", smd.Id, imd.Id)
	}
	if smd.OrgId != imd.OrgId {
		return fmt.Errorf("OrgId not equal. schema: %d, idx: %d", smd.OrgId, imd.OrgId)
	}
	if smd.Name != imd.Name.String() {
		return fmt.Errorf("Name not equal. schema: %v, idx: %v", smd.Name, imd.Name.String())
	}
	if smd.Interval != imd.Interval {
		return fmt.Errorf("Interval not equal. schema: %d, idx: %d", smd.Interval, imd.Interval)
	}
	if smd.Unit != imd.Unit {
		return fmt.Errorf("Unit not equal. schema: %v, idx: %v", smd.Unit, imd.Unit)
	}
	if smd.Mtype != imd.Mtype() {
		return fmt.Errorf("Mtype not equal. schema: %v, idx: %v", smd.Mtype, imd.Mtype())
	}
	imdTags := imd.Tags.Strings()
	if len(smd.Tags) != len(imdTags) {
		return fmt.Errorf("Length of tags not equal. schema: %d, idx: %d", len(smd.Tags), len(imdTags))
	}
	for i := 0; i < len(smd.Tags); i++ {
		if smd.Tags[i] != imdTags[i] {
			return fmt.Errorf("Tags not equal. schema: %v, idx: %v", smd.Tags[i], imdTags[i])
		}
	}
	if smd.LastUpdate != imd.LastUpdate {
		return fmt.Errorf("LastUpdate not equal. schema: %d, idx: %d", smd.LastUpdate, imd.LastUpdate)
	}
	if smd.Partition != imd.Partition {
		return fmt.Errorf("Partition not equal. schema: %d, idx: %d", smd.Partition, imd.Partition)
	}
	if smd.NameWithTags() != imd.NameWithTags() {
		return fmt.Errorf("NameWithTags() not equal. schema: %v, idx: %v", smd.NameWithTags(), imd.NameWithTags())
	}
	return nil
}

func TestMetricDefinitionConversion(t *testing.T) {
	idxMds := genMetricDefinitions(5)
	schemaMds := make([]schema.MetricDefinition, 5)
	for i := 0; i < 5; i++ {
		schemaMds[i] = idxMds[i].ConvertToSchemaMd()
		err := compareSchemaWithIdxMd(schemaMds[i], idxMds[i])
		if err != nil {
			t.Error(err)
		}
	}
}

func TestDump(t *testing.T) {
	idxMds := genMetricDefinitions(2)
	for _, md := range idxMds {
		Dump(md)
	}
}
