package metricstore

import (
	"bytes"
	"fmt"
	"encoding/json"
	"errors"
	"net/http"
	"time"
	"github.com/raintank/raintank-metric/metricdef"
)

// Kairosdb client
type Kairosdb struct {
	client *http.Client
	host   string
}

func NewKairosdb(host string) (*Kairosdb, error) {
	return &Kairosdb{
		client: &http.Client{Timeout: (10 * time.Second)},
		host: host,
	}, nil
}

// Datapoint instances are persisted back to kairosdb via AddDatapoints
type Datapoint struct {
	Name      string            `json:"name"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}


func (kdb *Kairosdb) SendMetrics(metrics *[]metricdef.IndvMetric) error {
	// marshal metrics into datapoint structs
	datapoints := make([]Datapoint, len(*metrics))
	for i, m := range *metrics {
		tags := make(map[string]string, len(m.Extra))
		for k,v := range m.Extra {
			tags[k] = fmt.Sprintf("%v", v)
		}
		datapoints[i] = Datapoint{
			Name: m.Name,
			Timestamp: m.Time,
			Value: m.Value,
			Tags: tags,
		}
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

