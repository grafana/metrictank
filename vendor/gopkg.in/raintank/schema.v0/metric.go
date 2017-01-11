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

//go:generate msgp

// MetricData contains all metric metadata and a datapoint
type MetricData struct {
	Id         string   `json:"id"`
	OrgId      int      `json:"org_id"`
	Name       string   `json:"name"`
	Metric     string   `json:"metric"`
	Interval   int      `json:"interval"`
	Value      float64  `json:"value"`
	Unit       string   `json:"unit"`
	Time       int64    `json:"time"`
	TargetType string   `json:"target_type"`
	Tags       []string `json:"tags" elastic:"type:string,index:not_analyzed"`
}

// returns a id (hash key) in the format OrgId.md5Sum
// the md5sum is a hash of the the concatination of the
// series name + each tag key:value pair, sorted alphabetically.
func (m *MetricData) SetId() {
	var buffer bytes.Buffer
	buffer.WriteString(m.Name)
	sort.Strings(m.Tags)
	for _, k := range m.Tags {
		buffer.WriteString(fmt.Sprintf(";%s", k))
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
	TargetType string            `json:"target_type"` // an emum ["derive","gauge"] in nodejs
	Tags       []string          `json:"tags" elastic:"type:string,index:not_analyzed"`
	LastUpdate int64             `json:"lastUpdate"` // unix epoch time, per the nodejs definition
	Nodes      map[string]string `json:"nodes"`
	NodeCount  int               `json:"node_count"`
}

func (m *MetricDefinition) SetId() {
	var buffer bytes.Buffer
	buffer.WriteString(m.Name)
	sort.Strings(m.Tags)
	for _, k := range m.Tags {
		buffer.WriteString(fmt.Sprintf(";%s", k))
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
		TargetType: d.TargetType,
		Interval:   d.Interval,
		LastUpdate: d.Time,
		Unit:       d.Unit,
		Tags:       tags,
		Nodes:      nodesMap,
		NodeCount:  len(nodes),
	}
}
