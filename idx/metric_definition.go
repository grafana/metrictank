package idx

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unsafe"

	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

type mType uint8

const (
	MTypeUndefined mType = iota
	MTypeGauge
	MTypeRate
	MTypeCount
	MTypeCounter
	MTypeTimestamp
)

//go:generate msgp
//msgppppp:shim MetricName as:string using:MetricName.String/MetricDefinition.SetMetricName

// MetricName stores uintptrs to strings interned in an object store
type MetricName struct {
	nodes []uintptr
}

func (mn *MetricName) setMetricName(name string) {
	nodes := strings.Split(name, ".")
	mn.nodes = make([]uintptr, len(nodes))
	for i, node := range nodes {
		// TODO: add error checking? Fail somehow
		nodePtr, _ := IdxIntern.AddOrGet([]byte(node))
		mn.nodes[i] = nodePtr
	}
}

func (mn *MetricName) String() string {
	if len(mn.nodes) == 0 {
		return ""
	}

	bld := strings.Builder{}
	return mn.string(&bld)
}

func (mn *MetricName) string(bld *strings.Builder) string {
	// get []int of the lengths of all of the mn.Nodes
	lns, ok := IdxIntern.LenNoCprsn(mn.nodes)
	if !ok {
		// this should never happen, do what now?
		return ""
	}

	// should be faster than calling IdxIntern.SetStringNoCprsn in a tight loop
	var tmpSz string
	szHeader := (*reflect.StringHeader)(unsafe.Pointer(&tmpSz))
	first, _ := IdxIntern.ObjString(mn.nodes[0])
	bld.WriteString(first)
	for idx, nodePtr := range mn.nodes[1:] {
		szHeader.Data = nodePtr
		szHeader.Len = lns[idx+1]
		bld.WriteString(".")
		bld.WriteString(tmpSz)
	}

	return bld.String()
}

func (mn *MetricName) ExtensionType() int8 {
	return 95
}

func (mn *MetricName) Len() int {
	return len(mn.String())
}

func (mn *MetricName) MarshalBinaryTo(b []byte) error {
	copy(b, []byte(mn.String()))
	return nil
}

func (mn *MetricName) UnmarshalBinary(b []byte) error {
	mn.setMetricName(string(b))
	return nil
}

// TagKeyValue holds interned versions of the Tag Key And Value
type TagKeyValue struct {
	Key   string
	Value string
}

func (t *TagKeyValue) String() string {
	bld := strings.Builder{}

	bld.WriteString(t.Key)
	bld.WriteString("=")
	bld.WriteString(t.Value)

	return bld.String()
}

// TagKeyValues stores a slice of all of the Tag K/V combinations for a MetricDefinition
type TagKeyValues []TagKeyValue

// Strings returns a slice containing all of the Tag K/V combinations for a MetricDefinition
func (t TagKeyValues) Strings() []string {
	tags := make([]string, len(t))
	for i, tag := range t {
		tags[i] = tag.String()
	}
	return tags
}

type MetricDefinition struct {
	Id         schema.MKey
	OrgId      uint32
	Name       MetricName `msg:"name,extension"`
	Interval   int
	Unit       string
	mtype      mType
	Tags       TagKeyValues
	LastUpdate int64
	Partition  int32
}

func (md *MetricDefinition) NameWithTags() string {
	bld := strings.Builder{}

	md.Name.string(&bld)
	for _, tag := range md.Tags {
		if tag.Key == "name" {
			continue
		}
		bld.WriteString(";")
		bld.WriteString(tag.String())
	}
	return bld.String()
}

func (md *MetricDefinition) SetMType(mtype string) {
	switch mtype {
	case "gauge":
		md.mtype = MTypeGauge
	case "rate":
		md.mtype = MTypeRate
	case "count":
		md.mtype = MTypeCount
	case "counter":
		md.mtype = MTypeCounter
	case "timestamp":
		md.mtype = MTypeTimestamp
	default:
		// for values "" and other unknown/corrupted values
		md.mtype = MTypeUndefined
	}
}

func (md *MetricDefinition) Mtype() string {
	switch md.mtype {
	case MTypeGauge:
		return "gauge"
	case MTypeRate:
		return "rate"
	case MTypeCount:
		return "count"
	case MTypeCounter:
		return "counter"
	case MTypeTimestamp:
		return "timestamp"
	default:
		// case of MTypeUndefined and also default for unknown/corrupted values
		return ""
	}
}

func (md *MetricDefinition) SetUnit(unit string) {
	sz, err := IdxIntern.AddOrGetSzNoCprsn([]byte(unit))
	if err != nil {
		md.Unit = unit
	}
	md.Unit = sz
}

func (md *MetricDefinition) SetMetricName(name string) {
	nodes := strings.Split(name, ".")
	md.Name.nodes = make([]uintptr, len(nodes))
	for i, node := range nodes {
		// TODO: add error checking? Fail somehow
		nodePtr, _ := IdxIntern.AddOrGet([]byte(node))
		md.Name.nodes[i] = nodePtr
	}
}

func (md *MetricDefinition) SetTags(tags []string) {
	md.Tags = make([]TagKeyValue, len(tags))
	sort.Strings(tags)
	for i, tag := range tags {
		splits := strings.Split(tag, "=")
		if len(splits) < 2 {
			invalidTag.Inc()
			log.Errorf("idx: Tag %q has an invalid format, ignoring", tag)
			continue
		}
		keySz, err := IdxIntern.AddOrGetSzNoCprsn([]byte(splits[0]))
		if err != nil {
			keyTmpSz := splits[0]
			md.Tags[i].Key = keyTmpSz
		} else {
			md.Tags[i].Key = keySz
		}

		valueSz, err := IdxIntern.AddOrGetSzNoCprsn([]byte(splits[1]))
		if err != nil {
			valueTmpSz := splits[1]
			md.Tags[i].Value = valueTmpSz
		} else {
			md.Tags[i].Value = valueSz
		}
	}
}

func MetricDefinitionFromMetricDataWithMkey(mkey schema.MKey, d *schema.MetricData) *MetricDefinition {
	md := &MetricDefinition{
		Id:         mkey,
		OrgId:      uint32(d.OrgId),
		Interval:   d.Interval,
		LastUpdate: d.Time,
	}

	md.SetMetricName(d.Name)
	md.SetMType(d.Mtype)
	md.SetTags(d.Tags)
	md.SetUnit(d.Unit)

	return md
}

func MetricDefinitionFromMetricData(d *schema.MetricData) (*MetricDefinition, error) {
	mkey, err := schema.MKeyFromString(d.Id)
	if err != nil {
		return nil, fmt.Errorf("Error parsing ID: %s", err)
	}

	return MetricDefinitionFromMetricDataWithMkey(mkey, d), nil
}
