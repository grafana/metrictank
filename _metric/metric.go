package metricdef

import (
	"fmt"
	elastigo "github.com/mattbaird/elastigo/lib"
	"log"
	"time"
)

type MetricDefinition struct {
	ID string
	Name string
	Account int
	Location string
	Metric string
	TargetType string // an emum ["derive","gauge"] in nodejs
	Unit string
	Interval int // minimum 10
	Site int
	LastUpdate int64 // unix epoch time, per the nodejs definition
	Monitor int
	WarnMin int
	WarnMax int
	CritMin int
	CritMax int
	KeepAlive bool
	State int8
}

var es *elastigo.Conn

func init() {
	es = elastigo.NewConn()
	es.Domain = "elasi_domain" // needs to be configurable obviously
}

// required: name, account, target_type, interval, metric, unit

// These validate, and save to elasticsearch

func (m *MetricDefinition) Save() error {
	if m.ID == "" {
		m.ID = fmt.Sprintf("%d.%s", m.Account, m.Name)
	}
	if m.LastUpdate == 0 {
		m.LastUpdate = time.Now().Unix()
	}
	if err := m.validate(); err != nil {
		return err
	}
	// save in elasticsearch
	return m.indexMetric()
}

func (m *MetricDefinition) Update() error {
	if err := m.validate(); err != nil {
		return nil
	}
	// save in elasticsearch
	return m.indexMetric()
}

func (m *MetricDefinition) validate() error {
	if m.Name == "" || m.Account == 0 || (m.TargetType != "derive" && m.TargetType != "gauge") || m.Interval == 0 || m.Metric == "" || m.Unit == "" {
		// TODO: this error message ought to be more informative
		err := fmt.Errorf("metric is not valid!")
		return err
	}
	return nil
}

func (m *MetricDefinition) indexMetric() error {
	resp, err := es.Index("definitions", "metric", m.ID, nil, m)
	log.Printf("response ok? %v", resp.Ok)
	if err != nil {
		return err
	}
	return nil
}

func GetMetricDefinition(id string) (*MetricDefinition, error) {
	// TODO: fetch from redis before checking elasticsearch

	res, err := es.Get("definitions", "metric", id, nil)
	if err != nil {
		return nil, err
	}
	log.Printf("get returned %q", res.Source)

	return nil, nil
}

func FindMetricDefinitions(filter, size string) ([]*MetricDefinition, error) {
	log.Printf("searching for %s", filter)
	body = make(map[string]interface{})
	body["query"] = filter
	body["size"] = size
	sort = make(map[string]map[string]string)
	sort["name"] = map[string]string{"order":"desc"}
	body["sort"] = []map[string]map[string]string{sort}

	res, err := es.Search("definitions", "metric", nil, body)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// temp: show us what we have before creating the objects from json
	for _, r := range res {
		log.Printf("returned: %q", r.RawJSON)
	}

	return nil, nil
}
