package metricdef

import (
	"encoding/json"
	"fmt"
	elastigo "github.com/mattbaird/elastigo/lib"
	"log"
	"time"
)

type MetricDefinition struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Account    int    `json:"account"`
	Location   string `json:"location`
	Metric     string `json:"metric"`
	TargetType string `json:"target_type"` // an emum ["derive","gauge"] in nodejs
	Unit       string `json:"unit"`
	Interval   int    `json:"interval"` // minimum 10
	Site       int    `json:"site"`
	LastUpdate int64  `json:"lastUpdate"` // unix epoch time, per the nodejs definition
	Monitor    int    `json:"monitor"`
	Thresholds struct {
		WarnMin interface{} `json:"warnMin"`
		WarnMax interface{} `json:"warnMax"`
		CritMin interface{} `json:"critMin"`
		CritMax interface{} `json:"critMax"`
	} `json:"thresholds"`
	KeepAlives int  `json:"keepAlives"`
	State      int8 `json:"state"`
}

var es *elastigo.Conn

func init() {
	es = elastigo.NewConn()
	es.Domain = "elasticsearch" // needs to be configurable obviously
	// TODO: once this has gotten far enough to be able to start running
	// on its own without running in tandem with the nodejs client, the
	// elasticsearch indexes will need to be checked for existence and
	// created if necessary.
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

func NewFromMessage(m map[string]interface{}) (*MetricDefinition, error) {
	log.Printf("incoming message: %+v", m)
	id := fmt.Sprintf("%d.%s", int64(m["account"].(float64)), m["name"])
	now := time.Now().Unix()

	var ka int
	if k, ok := m["keepAlives"].(float64) {
		ka = int(k)
	}

	// Thorough validation of the input needed once it's working.
	def := &MetricDefinition{ID: id, Name: m["name"].(string), Account: int(m["account"].(float64)), Location: m["location"].(string), Metric: m["metric"].(string), TargetType: m["target_type"].(string), Interval: int(m["interval"].(float64)), Site: int(m["site"].(float64)), LastUpdate: now, Monitor: int(m["monitor"].(float64)), KeepAlives: int(ka), State: int8(m["state"].(float64))}

	if t, exists := m["thresholds"]; exists {
		thresh, _ := t.(map[string]interface{})
		for k, v := range thresh {
			switch k {
			case "warnMin":
				def.Thresholds.WarnMin = int(v.(float64))
			case "warnMax":
				def.Thresholds.WarnMax = int(v.(float64))
			case "critMin":
				def.Thresholds.CritMin = int(v.(float64))
			case "critMax":
				def.Thresholds.CritMax = int(v.(float64))
			}
		}
	}

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
		return err
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
	log.Printf("res is: %+v", res)
	if err != nil {
		return nil, err
	}
	log.Printf("get returned %q", res.Source)
	def, err := DefFromJSON(*res.Source)
	if err != nil {
		return nil, err
	}

	return def, nil
}

func FindMetricDefinitions(filter, size string) ([]*MetricDefinition, error) {
	log.Printf("searching for %s", filter)
	body := make(map[string]interface{})
	body["query"] = filter
	body["size"] = size
	sort := make(map[string]map[string]string)
	sort["name"] = map[string]string{"order": "desc"}
	body["sort"] = []map[string]map[string]string{sort}

	res, err := es.Search("definitions", "metric", nil, body)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// temp: show us what we have before creating the objects from json
	// TODO: once we have that, render the objects
	log.Printf("returned: %q", res.RawJSON)

	return nil, nil
}
