package schema

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

var errInvalidIntervalzero = errors.New("interval cannot be 0")
var errInvalidOrgIdzero = errors.New("org-id cannot be 0")
var errInvalidEmptyName = errors.New("name cannot be empty")
var errInvalidEmptyMetric = errors.New("metric cannot be empty")
var errInvalidMtype = errors.New("invalid mtype")
var errInvalidTagFormat = errors.New("invalid tag format")

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
}

//go:generate msgp

// MetricData contains all metric metadata (some as fields, some as tags) and a datapoint
type MetricData struct {
	Id       string   `json:"id"`
	OrgId    int      `json:"org_id"`
	Name     string   `json:"name"`
	Metric   string   `json:"metric"`
	Interval int      `json:"interval"`
	Value    float64  `json:"value"`
	Unit     string   `json:"unit"`
	Time     int64    `json:"time"`
	Mtype    string   `json:"mtype"`
	Tags     []string `json:"tags" elastic:"type:string,index:not_analyzed"`
}

func (m *MetricData) Validate() error {
	if m.OrgId == 0 {
		return errInvalidOrgIdzero
	}
	if m.Interval == 0 {
		return errInvalidIntervalzero
	}
	if m.Name == "" {
		return errInvalidEmptyName
	}
	if m.Metric == "" {
		return errInvalidEmptyMetric
	}
	if m.Mtype == "" || (m.Mtype != "gauge" && m.Mtype != "rate" && m.Mtype != "count" && m.Mtype != "counter" && m.Mtype != "timestamp") {
		return errInvalidMtype
	}
	if !validateTags(m.Tags) {
		return errInvalidTagFormat
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

// returns a id (hash key) in the format OrgId.md5Sum
// the md5sum is a hash of the the concatination of the
// metric + each tag key:value pair (in metrics2.0 sense, so also fields), sorted alphabetically.
func (m *MetricData) SetId() {
	sort.Strings(m.Tags)

	buffer := bytes.NewBufferString(m.Metric)
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

// for ES
type MetricDefinition struct {
	Id         string   `json:"id"`
	OrgId      int      `json:"org_id"`
	Name       string   `json:"name" elastic:"type:string,index:not_analyzed"` // graphite format
	Metric     string   `json:"metric"`                                        // kairosdb format (like graphite, but not including some tags)
	Interval   int      `json:"interval"`                                      // minimum 10
	Unit       string   `json:"unit"`
	Mtype      string   `json:"mtype"`
	Tags       []string `json:"tags" elastic:"type:string,index:not_analyzed"`
	LastUpdate int64    `json:"lastUpdate"` // unix timestamp
	Partition  int32    `json:"partition"`
}

func (m *MetricDefinition) SetId() {
	sort.Strings(m.Tags)

	buffer := bytes.NewBufferString(m.Metric)
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

func (m *MetricDefinition) Validate() error {
	if m.OrgId == 0 {
		return errInvalidOrgIdzero
	}
	if m.Interval == 0 {
		return errInvalidIntervalzero
	}
	if m.Name == "" {
		return errInvalidEmptyName
	}
	if m.Metric == "" {
		return errInvalidEmptyMetric
	}
	if m.Mtype == "" || (m.Mtype != "gauge" && m.Mtype != "rate" && m.Mtype != "count" && m.Mtype != "counter" && m.Mtype != "timestamp") {
		return errInvalidMtype
	}
	if !validateTags(m.Tags) {
		return errInvalidTagFormat
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

func MetricDefinitionFromJSON(b []byte) (*MetricDefinition, error) {
	def := new(MetricDefinition)
	if err := json.Unmarshal(b, &def); err != nil {
		return nil, err
	}
	return def, nil
}

// MetricDefinitionFromMetricData yields a MetricDefinition that has no references
// to the original MetricData
func MetricDefinitionFromMetricData(d *MetricData) *MetricDefinition {
	tags := make([]string, len(d.Tags))
	copy(tags, d.Tags)
	return &MetricDefinition{
		Id:         d.Id,
		Name:       d.Name,
		OrgId:      d.OrgId,
		Metric:     d.Metric,
		Mtype:      d.Mtype,
		Interval:   d.Interval,
		LastUpdate: d.Time,
		Unit:       d.Unit,
		Tags:       tags,
	}
}

// validateTags verifies that all the tags are of a valid format. If one or more
// are invalid it returns false, otherwise true.
// a valid format is anything that looks like key=value, the length of key and
// value must be >0 and both must not contain the ; character.
func validateTags(tags []string) bool {
	for _, t := range tags {
		if len(t) == 0 {
			return false
		}

		// any = must not be the first character
		if t[0] == 61 {
			return false
		}

		foundEqual := false
		for pos := 0; pos < len(t); pos++ {
			// no ; allowed
			if t[pos] == 59 {
				return false
			}

			if !foundEqual {
				// no ! allowed in key
				if t[pos] == 33 {
					return false
				}

				// found the first =, so this will be the separator between key & value
				if t[pos] == 61 {
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
