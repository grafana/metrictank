package schema

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var errInvalidIntervalzero = errors.New("interval cannot be 0")
var errInvalidOrgIdzero = errors.New("org-id cannot be 0")
var errInvalidEmptyName = errors.New("name cannot be empty")
var errInvalidEmptyMetric = errors.New("metric cannot be empty")
var errInvalidMtype = errors.New("invalid mtype")

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
	return nil
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
	Id         string            `json:"id"`
	OrgId      int               `json:"org_id"`
	Name       string            `json:"name" elastic:"type:string,index:not_analyzed"` // graphite format
	Metric     string            `json:"metric"`                                        // kairosdb format (like graphite, but not including some tags)
	Interval   int               `json:"interval"`                                      // minimum 10
	Unit       string            `json:"unit"`
	Mtype      string            `json:"mtype"`
	Tags       []string          `json:"tags" elastic:"type:string,index:not_analyzed"`
	LastUpdate int64             `json:"lastUpdate"` // unix timestamp
	Nodes      map[string]string `json:"nodes"`
	NodeCount  int               `json:"node_count"`
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
	return nil
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
	nodesMap := make(map[string]string)
	nodes := strings.Split(d.Name, ".")
	for i, n := range nodes {
		key := fmt.Sprintf("n%d", i)
		nodesMap[key] = n
	}
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
		Nodes:      nodesMap,
		NodeCount:  len(nodes),
	}
}
