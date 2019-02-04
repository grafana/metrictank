package idx

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unsafe"

	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

//go:generate msgp

type mType uint8

// MType represented by a uint8
const (
	MTypeUndefined mType = iota
	MTypeGauge
	MTypeRate
	MTypeCount
	MTypeCounter
	MTypeTimestamp
)

// MetricName stores the name as a []uintptr to strings interned in an object store.
// Each word is stored as a separate interned string without the '.'. The whole name
// can be retrieved by calling MetricName.String()
type MetricName struct {
	nodes []uintptr
}

// Nodes returns the []uintptr of interned string addresses
// for the MetricName
func (mn *MetricName) Nodes() []uintptr {
	return mn.nodes
}

// String returns the full MetricName as a string
// using data interned in the object store
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

// setMetricName interns the MetricName in an
// object store and stores the addresses of those strings
// in MetricName.nodes
func (mn *MetricName) setMetricName(name string) {
	nodes := strings.Split(name, ".")
	mn.nodes = make([]uintptr, len(nodes))
	for i, node := range nodes {
		// TODO: add error checking? Fail somehow
		nodePtr, err := IdxIntern.AddOrGet([]byte(node))
		if err != nil {
			log.Error(err)
		}
		mn.nodes[i] = nodePtr
	}
}

// ExtensionType is required to use custom marshaling as an extension
// with msgp
func (mn *MetricName) ExtensionType() int8 {
	return 95
}

// Len is required to use custom marshaling as an extension
// with msgp
func (mn *MetricName) Len() int {
	return len(mn.String())
}

// MarshalBinaryTo is required to use custom marshaling as an extension
// in msgp
func (mn *MetricName) MarshalBinaryTo(b []byte) error {
	copy(b, []byte(mn.String()))
	return nil
}

// UnmarshalBinary is required to use custom marshaling as an extension
// in msgp
func (mn *MetricName) UnmarshalBinary(b []byte) error {
	mn.setMetricName(string(b))
	return nil
}

// TagKeyValue stores a Key/Value pair. The strings
// are interned in an object store before they are assigned.
type TagKeyValue struct {
	Key   string
	Value string
}

// String returns a Key/Value pair in the form of
// 'key=value'
func (t *TagKeyValue) String() string {
	bld := strings.Builder{}

	bld.WriteString(t.Key)
	bld.WriteString("=")
	bld.WriteString(t.Value)

	return bld.String()
}

// TagKeyValues stores a slice of all of the Tag Key/Value pair combinations for a MetricDefinition
type TagKeyValues []TagKeyValue

// Strings returns a slice containing all of the Tag Key/Value pair combinations for a MetricDefinition.
// Each item in the slice is in the form of 'key=value'
func (t TagKeyValues) Strings() []string {
	tags := make([]string, len(t))
	for i, tag := range t {
		tags[i] = tag.String()
	}
	return tags
}

// Helper functions to sort TagKeyValues
func (t TagKeyValues) Len() int           { return len(t) }
func (t TagKeyValues) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t TagKeyValues) Less(i, j int) bool { return t[i].Key < t[j].Key }

// MetricDefinition stores information which identifies a single metric
type MetricDefinition struct {
	Id    schema.MKey
	OrgId uint32
	// using custom marshalling for MetricName
	// if there is another way we should explore that
	Name       MetricName `msg:"name,extension"`
	Interval   int
	Unit       string
	mtype      mType
	Tags       TagKeyValues
	LastUpdate int64
	Partition  int32
}

// NameWithTags returns a string version of the MetricDefinition's name with
// all of its tagsin the form of 'name;key1=value1;key2=value2;key3=value3'
func (md *MetricDefinition) NameWithTags() string {
	bld := strings.Builder{}

	md.Name.string(&bld)
	sort.Sort(TagKeyValues(md.Tags))
	for _, tag := range md.Tags {
		if tag.Key == "name" {
			continue
		}
		bld.WriteString(";")
		bld.WriteString(tag.String())
	}
	return bld.String()
}

// SetMType translates a string into a uint8 which is used to store
// the actual metric type. Valid values are 'gauge', 'rate', 'count',
// 'counter', and 'timestamp'.
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

// Mtype returns a string version of the current MType
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

// SetUnit takes a string, interns it in an object store
// and then uses it to store the unit.
func (md *MetricDefinition) SetUnit(unit string) {
	sz, err := IdxIntern.AddOrGetSzNoCprsn([]byte(unit))
	if err != nil {
		log.Errorf("idx: Failed to intern Unit %v. %v", unit, err)
		md.Unit = unit
	}
	md.Unit = sz
}

// SetMetricName interns the MetricName in an
// object store and stores the addresses of those strings
// in MetricName.nodes
func (md *MetricDefinition) SetMetricName(name string) {
	nodes := strings.Split(name, ".")
	md.Name.nodes = make([]uintptr, len(nodes))
	for i, node := range nodes {
		// TODO: add error checking? Fail somehow
		nodePtr, err := IdxIntern.AddOrGet([]byte(node))
		if err != nil {
			log.Errorf("idx: Failed to intern word in MetricName: %v, %v", node, err)
		}
		md.Name.nodes[i] = nodePtr
	}
}

// SetTags takes a []string which should contain Key/Value pairs
// in the form of 'key=value'. It splits up the Key and Value for each
// item, interns them in the object store, and creates a TagKeyValue
// for them. It then stores all of these in Tags.
//
// The items in the input argument should not contain ';'. Each item
// is a separate Key/Value pair. Do not combine multiple Key/Value pairs
// into a single index in the []string.
func (md *MetricDefinition) SetTags(tags []string) {
	md.Tags = make([]TagKeyValue, len(tags))
	sort.Strings(tags)
	for i, tag := range tags {
		if strings.Contains(tag, ";") {
			invalidTag.Inc()
			log.Errorf("idx: Tag %q has an invalid format, ignoring", tag)
			continue
		}
		splits := strings.Split(tag, "=")
		if len(splits) < 2 {
			invalidTag.Inc()
			log.Errorf("idx: Tag %q has an invalid format, ignoring", tag)
			continue
		}
		keySz, err := IdxIntern.AddOrGetSzNoCprsn([]byte(splits[0]))
		if err != nil {
			log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
			keyTmpSz := splits[0]
			md.Tags[i].Key = keyTmpSz
		} else {
			md.Tags[i].Key = keySz
		}

		valueSz, err := IdxIntern.AddOrGetSzNoCprsn([]byte(splits[1]))
		if err != nil {
			log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
			valueTmpSz := splits[1]
			md.Tags[i].Value = valueTmpSz
		} else {
			md.Tags[i].Value = valueSz
		}
	}
}

// SetId creates and sets the MKey which identifies a metric
func (md *MetricDefinition) SetId() {
	sort.Sort(TagKeyValues(md.Tags))
	buffer := bytes.NewBufferString(md.Name.String())
	buffer.WriteByte(0)
	buffer.WriteString(md.Unit)
	buffer.WriteByte(0)
	buffer.WriteString(md.Mtype())
	buffer.WriteByte(0)
	fmt.Fprintf(buffer, "%d", md.Interval)

	for _, t := range md.Tags {
		if t.Key == "name" {
			continue
		}

		buffer.WriteByte(0)
		buffer.WriteString(t.String())
	}

	md.Id = schema.MKey{
		Key: md5.Sum(buffer.Bytes()),
		Org: uint32(md.OrgId),
	}
}

// MetricDefinitionFromMetricDataWithMkey takes an MKey and MetricData and returns a MetricDefinition
// based on them.
func MetricDefinitionFromMetricDataWithMkey(mkey schema.MKey, d *schema.MetricData) *MetricDefinition {
	md := &MetricDefinition{
		Id:         mkey,
		OrgId:      uint32(d.OrgId),
		Interval:   d.Interval,
		LastUpdate: d.Time,
	}

	md.SetMetricName(d.Name)
	md.SetUnit(d.Unit)
	md.SetMType(d.Mtype)
	md.SetTags(d.Tags)

	return md
}

// MetricDefinitionFromMetricData takes a MetricData, attempts to generate an MKey for it,
// and returns a MetricDefinition upon success. On failure it returns an error
func MetricDefinitionFromMetricData(d *schema.MetricData) (*MetricDefinition, error) {
	mkey, err := schema.MKeyFromString(d.Id)
	if err != nil {
		return nil, fmt.Errorf("idx: Error parsing ID: %s", err)
	}

	return MetricDefinitionFromMetricDataWithMkey(mkey, d), nil
}
