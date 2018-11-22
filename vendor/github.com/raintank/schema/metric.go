package schema

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

var ErrInvalidIntervalzero = errors.New("interval cannot be 0")
var ErrInvalidOrgIdzero = errors.New("org-id cannot be 0")
var ErrInvalidEmptyName = errors.New("name cannot be empty")
var ErrInvalidMtype = errors.New("invalid mtype")
var ErrInvalidTagFormat = errors.New("invalid tag format")

type PartitionedMetric interface {
	Validate() error
	SetId()
	// return a []byte key comprised of the metric's OrgId
	// accepts an input []byte to allow callers to re-use
	// buffers to reduce memory allocations
	KeyByOrgId([]byte) []byte
	// return a []byte key comprised of the metric's Name
	// accepts an input []byte to allow callers to re-use
	// buffers to reduce memory allocations
	KeyBySeries([]byte) []byte
	// return a []byte key comprised of the metric's Name with Tags
	// accepts an input []byte to allow callers to re-use
	// buffers to reduce memory allocations
	KeyBySeriesWithTags([]byte) []byte
}

//go:generate msgp

// MetricData contains all metric metadata (some as fields, some as tags) and a datapoint
type MetricData struct {
	Id       string   `json:"id"`
	OrgId    int      `json:"org_id"`
	Name     string   `json:"name"`
	Interval int      `json:"interval"`
	Value    float64  `json:"value"`
	Unit     string   `json:"unit"`
	Time     int64    `json:"time"`
	Mtype    string   `json:"mtype"`
	Tags     []string `json:"tags"`
}

func (m *MetricData) Validate() error {
	if m.OrgId == 0 {
		return ErrInvalidOrgIdzero
	}
	if m.Interval == 0 {
		return ErrInvalidIntervalzero
	}
	if m.Name == "" {
		return ErrInvalidEmptyName
	}
	if m.Mtype == "" || (m.Mtype != "gauge" && m.Mtype != "rate" && m.Mtype != "count" && m.Mtype != "counter" && m.Mtype != "timestamp") {
		return ErrInvalidMtype
	}
	if !ValidateTags(m.Tags) {
		return ErrInvalidTagFormat
	}
	return nil
}

func (m *MetricData) KeyByOrgId(b []byte) []byte {
	if cap(b)-len(b) < 4 {
		// not enough unused space in the slice so we need to grow it.
		newBuf := make([]byte, len(b), len(b)+4)
		copy(newBuf, b)
		b = newBuf
	}
	// PutUint32 writes directly to the slice rather then appending.
	// so we need to set the length to 4 more bytes then it currently is.
	b = b[:len(b)+4]
	binary.LittleEndian.PutUint32(b[len(b)-4:], uint32(m.OrgId))
	return b
}

func (m *MetricData) KeyBySeries(b []byte) []byte {
	b = append(b, []byte(m.Name)...)
	return b
}

func (m *MetricData) KeyBySeriesWithTags(b []byte) []byte {
	nameWithTagsBuffer := bytes.NewBuffer(b)
	nameWithTagsBuffer.WriteString(m.Name)

	sort.Strings(m.Tags)
	for _, t := range m.Tags {
		if len(t) >= 5 && t[:5] == "name=" {
			continue
		}

		nameWithTagsBuffer.WriteString(";")
		nameWithTagsBuffer.WriteString(t)
	}

	return nameWithTagsBuffer.Bytes()
}

// returns a id (hash key) in the format OrgId.md5Sum
// the md5sum is a hash of the the concatination of the
// metric + each tag key:value pair (in metrics2.0 sense, so also fields), sorted alphabetically.
func (m *MetricData) SetId() {
	sort.Strings(m.Tags)

	buffer := bytes.NewBufferString(m.Name)
	buffer.WriteByte(0)
	buffer.WriteString(m.Unit)
	buffer.WriteByte(0)
	buffer.WriteString(m.Mtype)
	buffer.WriteByte(0)
	fmt.Fprintf(buffer, "%d", m.Interval)

	for _, k := range m.Tags {
		buffer.WriteByte(0)
		buffer.WriteString(k)
	}
	m.Id = fmt.Sprintf("%d.%x", m.OrgId, md5.Sum(buffer.Bytes()))
}

// can be used by some encoders, such as msgp
type MetricDataArray []*MetricData

type MetricDefinition struct {
	Id       MKey   `json:"mkey"`
	OrgId    uint32 `json:"org_id"`
	Name     string `json:"name"`
	Interval int    `json:"interval"`
	Unit     string `json:"unit"`
	Mtype    string `json:"mtype"`

	// some users of MetricDefinition (f.e. MetricTank) might add a "name" tag
	// to this slice which allows querying by name as a tag. this special tag
	// should not be stored or transmitted over the network, otherwise it may
	// just get overwritten by the receiver.
	Tags       []string `json:"tags"`
	LastUpdate int64    `json:"lastUpdate"` // unix timestamp
	Partition  int32    `json:"partition"`

	// this is a special attribute that does not need to be set, it is only used
	// to keep the state of NameWithTags()
	nameWithTags string `json:"-"`
}

// NameWithTags deduplicates the name and tags strings by storing their content
// as a single string in .nameWithTags and then makes .Name and the .Tags slices
// of it. Once this has been done it won't do it a second time, but just reuse
// the already generated .nameWithTags.
// It returns the deduplicated name with tags.
func (m *MetricDefinition) NameWithTags() string {
	if len(m.nameWithTags) > 0 {
		return m.nameWithTags
	}

	sort.Strings(m.Tags)

	nameWithTagsBuffer := bytes.NewBufferString(m.Name)
	tagPositions := make([]int, 0, len(m.Tags)*2)
	for _, t := range m.Tags {
		if len(t) >= 5 && t[:5] == "name=" {
			continue
		}

		nameWithTagsBuffer.WriteString(";")
		tagPositions = append(tagPositions, nameWithTagsBuffer.Len())
		nameWithTagsBuffer.WriteString(t)
		tagPositions = append(tagPositions, nameWithTagsBuffer.Len())
	}

	m.nameWithTags = nameWithTagsBuffer.String()
	m.Tags = make([]string, len(tagPositions)/2)
	for i := 0; i < len(m.Tags); i++ {
		m.Tags[i] = m.nameWithTags[tagPositions[i*2]:tagPositions[i*2+1]]
	}
	m.Name = m.nameWithTags[:len(m.Name)]

	return m.nameWithTags
}

func (m *MetricDefinition) SetId() {
	sort.Strings(m.Tags)

	buffer := bytes.NewBufferString(m.Name)
	buffer.WriteByte(0)
	buffer.WriteString(m.Unit)
	buffer.WriteByte(0)
	buffer.WriteString(m.Mtype)
	buffer.WriteByte(0)
	fmt.Fprintf(buffer, "%d", m.Interval)

	for _, t := range m.Tags {
		if len(t) >= 5 && t[:5] == "name=" {
			continue
		}

		buffer.WriteByte(0)
		buffer.WriteString(t)
	}

	m.Id = MKey{
		md5.Sum(buffer.Bytes()),
		uint32(m.OrgId),
	}
}

func (m *MetricDefinition) Validate() error {
	if m.OrgId == 0 {
		return ErrInvalidOrgIdzero
	}
	if m.Interval == 0 {
		return ErrInvalidIntervalzero
	}
	if m.Name == "" {
		return ErrInvalidEmptyName
	}
	if m.Mtype == "" || (m.Mtype != "gauge" && m.Mtype != "rate" && m.Mtype != "count" && m.Mtype != "counter" && m.Mtype != "timestamp") {
		return ErrInvalidMtype
	}
	if !ValidateTags(m.Tags) {
		return ErrInvalidTagFormat
	}
	return nil
}

func (m *MetricDefinition) KeyByOrgId(b []byte) []byte {
	if cap(b)-len(b) < 4 {
		// not enough unused space in the slice so we need to grow it.
		newBuf := make([]byte, len(b), len(b)+4)
		copy(newBuf, b)
		b = newBuf
	}
	// PutUint32 writes directly to the slice rather then appending.
	// so we need to set the length to 4 more bytes then it currently is.
	b = b[:len(b)+4]
	binary.LittleEndian.PutUint32(b[len(b)-4:], uint32(m.OrgId))
	return b
}

func (m *MetricDefinition) KeyBySeries(b []byte) []byte {
	b = append(b, []byte(m.Name)...)
	return b
}

func (m *MetricDefinition) KeyBySeriesWithTags(b []byte) []byte {
	return append(b, []byte(m.NameWithTags())...)
}

// MetricDefinitionFromMetricData yields a MetricDefinition that has no references
// to the original MetricData
func MetricDefinitionFromMetricData(d *MetricData) *MetricDefinition {
	tags := make([]string, len(d.Tags))
	copy(tags, d.Tags)
	mkey, _ := MKeyFromString(d.Id)

	md := &MetricDefinition{
		Id:         mkey,
		Name:       d.Name,
		OrgId:      uint32(d.OrgId),
		Mtype:      d.Mtype,
		Interval:   d.Interval,
		LastUpdate: d.Time,
		Unit:       d.Unit,
		Tags:       tags,
	}

	return md
}

// ValidateTags returns whether all tags are in a valid format.
// a valid format is anything that looks like key=value,
// the length of key and value must be >0 and both must not contain the ; character.
func ValidateTags(tags []string) bool {
	for _, t := range tags {
		if len(t) == 0 {
			return false
		}

		if t[0] == '=' {
			return false
		}

		foundEqual := false
		for pos := 0; pos < len(t); pos++ {
			if t[pos] == ';' {
				return false
			}

			if !foundEqual {
				// no ! allowed in key
				if t[pos] == '!' {
					return false
				}

				// found the first =, so this will be the separator between key & value
				if t[pos] == '=' {
					// first equal sign must not be the last character
					if pos == len(t)-1 {
						return false
					}

					foundEqual = true
				}
			}
		}

		if !foundEqual {
			return false
		}
	}

	return true
}
