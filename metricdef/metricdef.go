package metricdef

import (
	"encoding/json"
	"fmt"
	elastigo "github.com/mattbaird/elastigo/lib"
	"log"
	"time"
)

type MetricDefinition struct {
	ID string `json:"id"`
	Name string `json:"name"`
	Account int `json:"account"`
	Location string `json:"location`
	Metric string `json:"metric"`
	TargetType string `json:"target_type"` // an emum ["derive","gauge"] in nodejs
	Unit string `json:"unit"`
	Interval int `json:"interval"` // minimum 10
	Site int `json:"site"`
	LastUpdate int64 `json:"lastUpdate"`// unix epoch time, per the nodejs definition
	Monitor int `json:"monitor"`
	Thresholds struct {
		WarnMin int `json:"warnMin"`
		WarnMax int `json:"warnMax"`
		CritMin int `json:"critMin"`
		CritMax int `json:"critMax"`
	} `json:"thresholds"`
	KeepAlive bool `json:"keepAlives"`
	State int8 `json:"state"`
}

var es *elastigo.Conn

func init() {
	es = elastigo.NewConn()
	es.Domain = "elasi_domain" // needs to be configurable obviously
}

// required: name, account, target_type, interval, metric, unit

// These validate, and save to elasticsearch

func DefFromJSON(b []byte) (*MetricDefinition, error) {
	def := new(MetricDefinition)
	if err := json.Unmarshal(b, &def); err != nil {
		return nil, err
	}
	def.ID = fmt.Sprintf("%d.%s", def.Account, def.Name)
	return def, nil
}

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
	body := make(map[string]interface{})
	body["query"] = filter
	body["size"] = size
	sort := make(map[string]map[string]string)
	sort["name"] = map[string]string{"order":"desc"}
	body["sort"] = []map[string]map[string]string{sort}

	res, err := es.Search("definitions", "metric", nil, body)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// temp: show us what we have before creating the objects from json
	log.Printf("returned: %q", res.RawJSON)

	return nil, nil
}
