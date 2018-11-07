package idx

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"

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

var (
	stringBuilders = sync.Pool{New: func() interface{} { return &strings.Builder{} }}
)

func returnStringBuilder(builder *strings.Builder) {
	builder.Reset()
	stringBuilders.Put(builder)
}

//go:generate msgp
type MetricName struct {
	nodes []string
}

func (mn *MetricName) String() string {
	if len(mn.nodes) == 0 {
		return ""
	}

	builder := stringBuilders.Get().(*strings.Builder)
	defer returnStringBuilder(builder)

	return mn.string(builder)
}

func (mn *MetricName) string(builder *strings.Builder) string {
	builder.WriteString(mn.nodes[0])
	for _, node := range mn.nodes[1:] {
		builder.WriteString(".")
		builder.WriteString(node)
	}

	return builder.String()
}

type TagKeyValue struct {
	Key   string
	Value string
}

func (t *TagKeyValue) String() string {
	builder := stringBuilders.Get().(*strings.Builder)
	defer returnStringBuilder(builder)

	builder.WriteString(t.Key)
	builder.WriteString("=")
	builder.WriteString(t.Value)

	return builder.String()
}

type TagKeyValues []TagKeyValue

func (t TagKeyValues) Strings() []string {
	tags := make([]string, len(t))
	for i, tag := range t {
		tags[i] = tag.String()
	}
	return tags
}

type MetricDefinition struct {
	Id       schema.MKey `json:"mkey"`
	OrgId    uint32      `json:"org_id"`
	Name     MetricName  `json:"name"`
	Interval int         `json:"interval"`
	Unit     string      `json:"unit"`
	mtype    mType       `json:"mtype"`

	// some users of MetricDefinition (f.e. MetricTank) might add a "name" tag
	// to this slice which allows querying by name as a tag. this special tag
	// should not be stored or transmitted over the network, otherwise it may
	// just get overwritten by the receiver.
	Tags       TagKeyValues `json:"tags"`
	LastUpdate int64        `json:"lastUpdate"` // unix timestamp
	Partition  int32        `json:"partition"`
}

func (md *MetricDefinition) NameWithTags() string {
	builder := stringBuilders.Get().(*strings.Builder)
	defer returnStringBuilder(builder)

	md.Name.string(builder)
	for _, tag := range md.Tags {
		builder.WriteString(tag.String())
	}
	return builder.String()
}

// delMetricDefinitionStrings updates the reference counts of each of the strings
// used in the given metric definition. it is only to be called by the GC before
// freeing an MD
func delMetricDefinitionStrings(md *MetricDefinition) {
	for _, tag := range md.Tags {
		stringPool.Del(tag.Key)
		stringPool.Del(tag.Value)
	}
	for _, node := range md.Name.nodes {
		stringPool.Del(node)
	}
	if len(md.Unit) > 0 {
		stringPool.Del(md.Unit)
	}
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
	md.Unit = stringPool.Add(unit)
}

func (md *MetricDefinition) SetMetricName(name string) {
	nodes := strings.Split(name, ".")
	md.Name.nodes = make([]string, len(nodes))
	for i, node := range nodes {
		md.Name.nodes[i] = stringPool.Add(node)
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
		md.Tags[i].Key = stringPool.Add(splits[0])
		md.Tags[i].Value = stringPool.Add(splits[1])
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

	runtime.SetFinalizer(md, delMetricDefinitionStrings)

	return md
}

func MetricDefinitionFromMetricData(d *schema.MetricData) (*MetricDefinition, error) {
	mkey, err := schema.MKeyFromString(d.Id)
	if err != nil {
		return nil, fmt.Errorf("Error parsing ID: %s", err)
	}

	return MetricDefinitionFromMetricDataWithMkey(mkey, d), nil
}
