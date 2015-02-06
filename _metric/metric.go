package metric

import (
	"time"
)

type MetricParent struct {
	Class string // an emum ["monitor","continuousQuery"] in nodejs
	ID int64
}

type Metric struct {
	ID string
	Name string
	Account int
	TargetType string // an emum ["monitor","continuousQuery"] in nodejs
	Units string
	Interval integer // minimum 10
	Parent *MetricParent
	WarnMin int
	WarnMax int
	CritMin int
	CritMax int
	KeepAlive bool
	State int8
}

// required: name, account, target_type, interval, parent

// These validate, and save to elasticsearch

func (m *Metric) Save() error {

}

func (m *Metric) Update() error {

}

func GetMetric(id string) (*Metric, error) {

}

func FindMetrics(filter, size string) ([]*Metric, error) {

}
