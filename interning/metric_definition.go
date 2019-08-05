package interning

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
)

//msgp:ignore Md5Hash

// Md5Hash is a structure for more compactly storing an md5 hash than using a string
type Md5Hash struct {
	Upper uint64
	Lower uint64
}

//go:generate msgp

func init() {
	msgp.RegisterExtension(95, func() msgp.Extension { return &MetricName{} })
	msgp.RegisterExtension(90, func() msgp.Extension { return &TagKeyValues{} })
	msgp.RegisterExtension(96, func() msgp.Extension { return new(Unit) })
}

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

var metricDefinitionInternedPool = sync.Pool{
	New: func() interface{} {
		return new(MetricDefinitionInterned)
	},
}

type Unit uintptr

func (u *Unit) ExtensionType() int8 {
	return 96
}

func (u *Unit) Len() int {
	return len(u.String())
}

func (u *Unit) MarshalBinaryTo(b []byte) error {
	copy(b, []byte(u.String()))
	return nil
}

func (u *Unit) UnmarshalBinary(value []byte) error {
	ptr, err := IdxIntern.AddOrGet(value, false)
	if err != nil {
		return fmt.Errorf("idx: Failed to get string from unit ptr: %s", err.Error())
	}
	*u = Unit(ptr)
	return nil
}

func (u *Unit) String() string {
	if *u == 0 {
		return ""
	}

	res, err := IdxIntern.GetStringFromPtr(uintptr(*u))
	if err != nil {
		log.Errorf("idx: Failed to acquire interned string from unit: %s", err)
		return ""
	}
	return res
}

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

	var bld strings.Builder
	return mn.string(&bld)
}

func (mn *MetricName) string(bld *strings.Builder) string {
	name, err := IdxIntern.JoinStrings(mn.nodes, ".")
	if err != nil {
		bld.WriteString("invalid")
		return bld.String()
	}
	bld.WriteString(name)
	return bld.String()
}

// setMetricName interns the MetricName in an
// object store and stores the addresses of those strings
// in MetricName.nodes
func (mn *MetricName) setMetricName(name string) error {
	nodes := strings.Split(name, ".")
	var offset int

	if len(nodes) > 3 {
		pre := strings.Join(nodes[:3], ".")
		nodePtr, err := IdxIntern.AddOrGet([]byte(pre), false)
		if err != nil {
			log.Error("idx: Failed to acquire interned string for node name: ", err)
			internError.Inc()
			return fmt.Errorf("idx: Failed to acquire interned string for first three nodes %v", err)
		}

		// still need to allocate for the first three nodes, which is now 1,
		// so we subtract 2
		mn.nodes = make([]uintptr, len(nodes)-2)
		mn.nodes[0] = nodePtr

		// re-slice and add all the other nodes
		nodes = nodes[3:]

		// add one to the offset
		offset++
	} else {
		mn.nodes = make([]uintptr, len(nodes))
	}

	for i, node := range nodes {
		nodePtr, err := IdxIntern.AddOrGet([]byte(node), false)
		if err != nil {
			log.Error("idx: Failed to acquire interned string for node name: ", err)
			internError.Inc()
			return fmt.Errorf("idx: Failed to acquire interned string for node %v, %v", node, err)
		}
		mn.nodes[i+offset] = nodePtr
	}

	return nil
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
	err := mn.setMetricName(string(b))
	return err
}

//msgp:shim TagKeyValue as:string using:TagKeyValue.createTagKeyValue/parseTagKeyValue

// TagKeyValue stores a Key/Value pair. The strings
// are interned in an object store before they are assigned.
type TagKeyValue struct {
	Key   uintptr
	Value uintptr
}

// String returns a Key/Value pair in the form of
// 'key=value'
func (t *TagKeyValue) String() string {
	var bld strings.Builder
	return t.string(&bld)
}

func (t *TagKeyValue) string(bld *strings.Builder) string {
	key, err := IdxIntern.GetStringFromPtr(t.Key)
	if err != nil {
		panic(fmt.Sprintf("idx: Failed to retrieve interned tag key: %v", err))
	}
	val, err := IdxIntern.GetStringFromPtr(t.Value)
	if err != nil {
		panic(fmt.Sprintf("idx: Failed to retrieve interned tag value: %v", err))
	}

	bld.WriteString(key)
	bld.WriteString("=")
	bld.WriteString(val)

	return bld.String()
}

func (t TagKeyValue) createTagKeyValue() string {
	bld := strings.Builder{}

	key, err := IdxIntern.GetStringFromPtr(t.Key)
	if err != nil {
		log.Error("idx: Failed to retrieve interned tag key: ", err)
		internError.Inc()
	}
	val, err := IdxIntern.GetStringFromPtr(t.Value)
	if err != nil {
		log.Error("idx: Failed to retrieve interned tag value: ", err)
		internError.Inc()
	}

	bld.WriteString(key)
	bld.WriteString("=")
	bld.WriteString(val)

	return bld.String()
}

func parseTagKeyValue(tag string) TagKeyValue {
	var tkv TagKeyValue
	eqPos := strings.Index(tag, "=")
	if eqPos < 0 {
		log.Errorf("idx: Tag %q has an invalid format, ignoring", tag)
		invalidTag.Inc()
		return TagKeyValue{}
	}

	if strings.ContainsAny(tag[:eqPos], "; ! ^ =") {
		log.Errorf("idx: Tag key %s has an invalid format, ignoring", tag[:eqPos])
		invalidTag.Inc()
		return TagKeyValue{}
	}
	key, err := IdxIntern.AddOrGet([]byte(tag[:eqPos]), true)
	if err != nil {
		log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
		internError.Inc()
		return TagKeyValue{}
	}
	tkv.Key = key

	if strings.ContainsAny(tag[eqPos+1:], "; ~") {
		log.Errorf("idx: Tag value %s has an invalid format, ignoring", tag[eqPos+1:])
		invalidTag.Inc()
		return TagKeyValue{}
	}
	value, err := IdxIntern.AddOrGet([]byte(tag[eqPos+1:]), true)
	if err != nil {
		log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
		internError.Inc()
		IdxIntern.Delete(key)
		return TagKeyValue{}
	}
	tkv.Value = value
	return tkv
}

type KeyValuesSlice []TagKeyValue

func (kvs KeyValuesSlice) Len() int      { return len(kvs) }
func (kvs KeyValuesSlice) Swap(i, j int) { kvs[i], kvs[j] = kvs[j], kvs[i] }
func (kvs KeyValuesSlice) Less(i, j int) bool {
	k1, _ := IdxIntern.GetStringFromPtr(kvs[i].Key)
	k2, _ := IdxIntern.GetStringFromPtr(kvs[j].Key)
	return k1 < k2
}

// TagKeyValues stores a slice of all of the Tag Key/Value pair combinations for a MetricDefinition
type TagKeyValues struct {
	KeyValues KeyValuesSlice
}

// Strings returns a slice containing all of the Tag Key/Value pair combinations for a MetricDefinition.
// Each item in the slice is in the form of 'key=value'
func (t *TagKeyValues) Strings() []string {
	tags := make([]string, len((*t).KeyValues))
	for i, tag := range (*t).KeyValues {
		tags[i] = tag.String()
	}
	return tags
}

func (t *TagKeyValues) ExtensionType() int8 {
	return 90
}

func (t *TagKeyValues) Len() int {
	var total int
	for _, kv := range t.Strings() {
		if strings.HasPrefix(kv, "name=") {
			continue
		}
		total += len(kv) + 1
	}
	return total
}

func (t *TagKeyValues) MarshalBinaryTo(b []byte) error {
	var total int
	for _, kv := range t.Strings() {
		if strings.HasPrefix(kv, "name=") {
			continue
		}
		if kv == "=" || kv == "" {
			continue
		}
		copy(b[total:], kv)
		total += len(kv)
		copy(b[total:], ";")
		total++
	}
	return nil
}

func (t *TagKeyValues) UnmarshalBinary(b []byte) error {
	if len(b) < 3 {
		return nil
	}
	tags := strings.Split(string(b[:len(b)-1]), ";")
	tmp := make([]TagKeyValue, len(tags))
	t.KeyValues = tmp
	for i, tag := range tags {
		if tag == "=" || tag == "" {
			log.Error("idx: Empty tag, ignoring: ", tag)
			invalidTag.Inc()
			continue
		}
		eqPos := strings.Index(tag, "=")
		if eqPos < 0 {
			log.Errorf("idx: Tag %q has an invalid format, ignoring", tag)
			invalidTag.Inc()
			continue
		}

		if strings.ContainsAny(tag[:eqPos], "; ! ^ =") {
			log.Errorf("idx: Tag key %s has an invalid format, ignoring", tag[:eqPos])
			invalidTag.Inc()
			continue
		}
		key, err := IdxIntern.AddOrGet([]byte(tag[:eqPos]), true)
		if err != nil {
			log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
			internError.Inc()
			continue
		}
		(*t).KeyValues[i].Key = key

		if strings.ContainsAny(tag[eqPos+1:], "; ~") {
			log.Errorf("idx: Tag value %s has an invalid format, ignoring", tag[eqPos+1:])
			invalidTag.Inc()
			continue
		}
		value, err := IdxIntern.AddOrGet([]byte(tag[eqPos+1:]), true)
		if err != nil {
			log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
			internError.Inc()
			IdxIntern.Delete(key)
			continue
		}
		(*t).KeyValues[i].Value = value
	}
	return nil
}

// MetricDefinitionInterned stores information which identifies a single metric
type MetricDefinitionInterned struct {
	Id    schema.MKey
	OrgId uint32
	// using custom marshalling for MetricName
	// if there is another way we should explore that
	Name       MetricName `msg:"name,extension"`
	Interval   int
	Unit       Unit `msg:"unit,extension"`
	mtype      mType
	Tags       TagKeyValues `msg:"tagkeyvalues,extension"`
	LastUpdate int64
	Partition  int32
}

// NameWithTags returns a string version of the MetricDefinitionInterned's
// name with all of its tags in the form of 'name;key1=value1;key2=value2;key3=value3'
func (md *MetricDefinitionInterned) NameWithTags() string {
	var bld strings.Builder

	md.Name.string(&bld)
	for _, tag := range md.Tags.KeyValues {
		key, err := IdxIntern.GetStringFromPtr(tag.Key)
		if err != nil {
			panic(fmt.Sprintf("idx: Failed to retrieve interned tag key: %v", err))
		}
		if key == "name" {
			continue
		}
		bld.WriteString(";")
		tag.string(&bld)
	}
	return bld.String()
}

// NameWithTagsHash returns an Md5Hash struct containing the
// hashed md5 sum of a NameWithTags for the given MetricDefinition
func (md *MetricDefinitionInterned) NameWithTagsHash() Md5Hash {
	md5Sum := md5.Sum([]byte(md.NameWithTags()))
	ret := Md5Hash{
		Upper: binary.LittleEndian.Uint64(md5Sum[:8]),
		Lower: binary.LittleEndian.Uint64(md5Sum[8:]),
	}
	return ret
}

// SetMType translates a string into a uint8 which is used to store
// the actual metric type. Valid values are 'gauge', 'rate', 'count',
// 'counter', and 'timestamp'.
func (md *MetricDefinitionInterned) SetMType(mtype string) {
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
func (md *MetricDefinitionInterned) Mtype() string {
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
func (md *MetricDefinitionInterned) SetUnit(unit string) {
	if unit == "" {
		md.Unit = 0
		return
	}

	sz, err := IdxIntern.AddOrGet([]byte(unit), false)
	if err != nil {
		log.Errorf("idx: Failed to intern Unit %v. %v", unit, err)
		internError.Inc()
		md.Unit = 0
		return
	}

	md.Unit = Unit(sz)
}

// SetMetricName interns the MetricName in an
// object store and stores the addresses of those strings
// in MetricName.nodes
func (md *MetricDefinitionInterned) SetMetricName(name string) error {
	nodes := strings.Split(name, ".")
	var offset int

	if len(nodes) > 3 {
		pre := strings.Join(nodes[:3], ".")
		nodePtr, err := IdxIntern.AddOrGet([]byte(pre), false)
		if err != nil {
			log.Error("idx: Failed to acquire interned string for node name: ", err)
			internError.Inc()
			return fmt.Errorf("idx: Failed to acquire interned string for first three nodes %v", err)
		}

		// still need to allocate for the first three nodes, which is now 1,
		// so we subtract 2
		md.Name.nodes = make([]uintptr, len(nodes)-2)
		md.Name.nodes[0] = nodePtr

		// re-slice and add all the other nodes
		nodes = nodes[3:]

		// add one to the offset
		offset++
	} else {
		md.Name.nodes = make([]uintptr, len(nodes))
	}

	for i, node := range nodes {
		nodePtr, err := IdxIntern.AddOrGet([]byte(node), false)
		if err != nil {
			log.Errorf("idx: Failed to intern word in MetricName: %v, %v", node, err)
			internError.Inc()
			return fmt.Errorf("idx: Failed to intern word in MetricName: %v, %v", node, err)
		}
		md.Name.nodes[i+offset] = nodePtr
	}

	return nil
}

// SetTags takes a []string which should contain Key/Value pairs
// in the form of 'key=value'. It splits up the Key and Value for each
// item, interns them in the object store, and creates a TagKeyValue
// for them. It then stores all of these in Tags.
//
// The items in the input argument should not contain ';'. Each item
// is a separate Key/Value pair. Do not combine multiple Key/Value pairs
// into a single index in the []string.
func (md *MetricDefinitionInterned) SetTags(tags []string) {
	md.Tags.KeyValues = make([]TagKeyValue, 0, len(tags))
	sort.Strings(tags)

	for _, tag := range tags {
		if tag == "=" || tag == "" {
			log.Error("idx: SetTags: Empty tag, ignoring: ", tag)
			invalidTag.Inc()
			continue
		}
		eqPos := strings.Index(tag, "=")
		if eqPos < 0 {
			log.Errorf("idx: SetTags: Tag %q has an invalid format, ignoring", tag)
			invalidTag.Inc()
			continue
		}

		if tag[:eqPos] == "name" {
			continue
		}

		if strings.ContainsAny(tag[:eqPos], "; ! ^ =") {
			log.Errorf("idx: Tag key %s has an invalid format, ignoring", tag[:eqPos])
			invalidTag.Inc()
			continue
		}
		key, err := IdxIntern.AddOrGet([]byte(tag[:eqPos]), true)
		if err != nil {
			log.Errorf("idx: SetTags: Failed to intern tag %q, %v", tag, err)
			internError.Inc()
			continue
		}

		if strings.ContainsAny(tag[eqPos+1:], "; ~") {
			log.Errorf("idx: Tag value %s has an invalid format, ignoring", tag[eqPos+1:])
			invalidTag.Inc()
			continue
		}
		value, err := IdxIntern.AddOrGet([]byte(tag[eqPos+1:]), true)
		if err != nil {
			log.Errorf("idx: SetTags: Failed to intern tag %q, %v", tag, err)
			internError.Inc()
			IdxIntern.Delete(key)
			continue
		}

		md.Tags.KeyValues = append(md.Tags.KeyValues, TagKeyValue{Key: key, Value: value})
	}
}

// SetId creates and sets the MKey which identifies a metric
func (md *MetricDefinitionInterned) SetId() {
	sort.Sort(md.Tags.KeyValues)
	buffer := bytes.NewBufferString(md.Name.String())
	buffer.WriteByte(0)
	buffer.WriteString(md.Unit.String())
	buffer.WriteByte(0)
	buffer.WriteString(md.Mtype())
	buffer.WriteByte(0)
	fmt.Fprintf(buffer, "%d", md.Interval)

	for _, t := range md.Tags.KeyValues {
		key, err := IdxIntern.GetStringFromPtr(t.Key)
		if err != nil {
			log.Error("idx: Failed to retrieve interned tag key: ", err)
			internError.Inc()
			continue
		}
		if key == "name" {
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

// ConvertToSchemaMd converts an idx.MetricDefinition to a schema.MetricDefinition
func (md *MetricDefinitionInterned) ConvertToSchemaMd() schema.MetricDefinition {
	smd := schema.MetricDefinition{
		Id:         md.Id,
		OrgId:      md.OrgId,
		Name:       md.Name.String(),
		Interval:   md.Interval,
		Unit:       md.Unit.String(),
		Mtype:      md.Mtype(),
		Tags:       md.Tags.Strings(),
		LastUpdate: atomic.LoadInt64(&md.LastUpdate),
		Partition:  atomic.LoadInt32(&md.Partition),
	}
	return smd
}

// CloneInterned() creates a new safe copy of the interned
// archive. A safe copy in this context means that when accessing
// the copy one does not need to worry about atomics or the string
// interning.
// It is important that .ReleaseInterned() gets called before it
// goes out of scope to return its memory back to the pools and to
// update the reference counts of interned values correctly
func (md *MetricDefinitionInterned) CloneInterned() *MetricDefinitionInterned {
	clone := metricDefinitionInternedPool.Get().(*MetricDefinitionInterned)
	clone.Id = md.Id
	clone.OrgId = md.OrgId
	clone.Name = md.Name
	clone.Interval = md.Interval
	clone.Unit = md.Unit
	clone.mtype = md.mtype
	clone.Tags = md.Tags
	clone.LastUpdate = atomic.LoadInt64(&md.LastUpdate)
	clone.Partition = atomic.LoadInt32(&md.Partition)

	IdxIntern.IncRefCntBatchUnsafe(clone.Name.Nodes())
	for i := range clone.Tags.KeyValues {
		IdxIntern.IncRefCntBatchUnsafe([]uintptr{clone.Tags.KeyValues[i].Key, clone.Tags.KeyValues[i].Value})
	}

	if clone.Unit != 0 {
		IdxIntern.IncRefCntUnsafe(uintptr(clone.Unit))
	}

	return clone
}

// ReleaseInterned() should be called whenever an instance of
// ArchiveInterned goes out of scope (before it gets GCed).
// It is also improtant that ReleaseInterned() only gets called
// exactly once when an ArchiveInterned goes out of scope,
// and not more than that.
// It updates the refence counts of the interned struct
// properties, or deletes the interned values when necessary.
func (md *MetricDefinitionInterned) ReleaseInterned() {
	IdxIntern.DeleteBatchUnsafe(md.Name.Nodes())
	for i := range md.Tags.KeyValues {
		IdxIntern.DeleteBatchUnsafe([]uintptr{md.Tags.KeyValues[i].Key, md.Tags.KeyValues[i].Value})
	}

	if md.Unit != 0 {
		IdxIntern.DeleteUnsafe(uintptr(md.Unit))
	}

	metricDefinitionInternedPool.Put(md)
}

// MetricDefinitionFromMetricDataWithMKey takes an MKey and MetricData and returns a MetricDefinition
// based on them.
func MetricDefinitionFromMetricDataWithMKey(mkey schema.MKey, d *schema.MetricData) (*MetricDefinitionInterned, error) {
	md := &MetricDefinitionInterned{
		Id:         mkey,
		OrgId:      uint32(d.OrgId),
		Interval:   d.Interval,
		LastUpdate: d.Time,
	}

	err := md.SetMetricName(d.Name)
	if err != nil {
		return &MetricDefinitionInterned{}, err
	}
	md.SetUnit(d.Unit)
	md.SetMType(d.Mtype)
	md.SetTags(d.Tags)

	return md, nil
}

// MetricDefinitionFromMetricData takes a MetricData, attempts to generate an MKey for it,
// and returns a MetricDefinition upon success. On failure it returns an error
func MetricDefinitionFromMetricData(d *schema.MetricData) (*MetricDefinitionInterned, error) {
	mkey, err := schema.MKeyFromString(d.Id)
	if err != nil {
		return nil, fmt.Errorf("idx: Error parsing ID: %s", err)
	}

	return MetricDefinitionFromMetricDataWithMKey(mkey, d)
}
