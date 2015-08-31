package metricstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ctdk/goas/v2/logger"
	"github.com/raintank/raintank-metric/schema"
)

// Kairosdb client
type Kairosdb struct {
	client *http.Client
	host   string
}

func NewKairosdb(host string) (*Kairosdb, error) {
	logger.Debugf("initializing kairosdb client to %s", host)
	return &Kairosdb{
		client: &http.Client{Timeout: (10 * time.Second)},
		host:   host,
	}, nil
}

// Datapoint instances are persisted back to kairosdb via AddDatapoints
type Datapoint struct {
	Name      string            `json:"name"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}

func MetricToDataPoint(m schema.MetricData) Datapoint {
	tags := make(map[string]string)
	for _, t := range m.Tags {
		parts := strings.Split(t, ":")
		if len(parts) == 2 {
			tags[parts[0]] = tags[parts[1]]
		}
	}
	tags["org_id"] = fmt.Sprintf("%v", m.OrgId)
	return Datapoint{
		Name:      m.Metric,
		Timestamp: m.Time * 1000,
		Value:     m.Value,
		Tags:      tags,
	}
}

func (kdb *Kairosdb) SendMetricPointers(metrics []*schema.MetricData) error {
	datapoints := make([]Datapoint, len(metrics))
	for i, m := range metrics {
		datapoints[i] = MetricToDataPoint(*m)
	}
	return kdb.AddDatapoints(datapoints)
}

func (kdb *Kairosdb) SendMetrics(metrics *[]schema.MetricData) error {
	datapoints := make([]Datapoint, len(*metrics))
	for i, m := range *metrics {
		datapoints[i] = MetricToDataPoint(m)
	}
	return kdb.AddDatapoints(datapoints)
}

// AddDatapoints add datapoints to configured kairosdb instance
func (kdb *Kairosdb) AddDatapoints(datapoints []Datapoint) error {

	json, err := json.Marshal(datapoints)
	if err != nil {
		return err
	}
	resp, err := kdb.client.Post(kdb.host+"/api/v1/datapoints", "application/json", bytes.NewBuffer(json))
	if err != nil {
		return err
	}
	if resp.Status != "204 No Content" {
		return errors.New("Response was non-200: " + resp.Status)
	}
	return nil
}

func (kdb *Kairosdb) Type() string {
	return "Kairosdb"
}
