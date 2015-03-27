package metricstore

import (
	"strconv"
	"github.com/marpaia/graphite-golang"
	"github.com/raintank/raintank-metric/metricdef"
)

// Kairosdb client
type Influxdb struct {
	Graphite *graphite.Graphite
}

func NewInfluxdb(host string, port int) (*Influxdb, error) {
	graphite, err := graphite.NewGraphite(host, port)
	if err != nil {
		return nil, err
	} 
	return &Influxdb{ Graphite: graphite }, nil
}

func (influx Influxdb) SendMetrics(metrics *[]metricdef.IndvMetric) error {
	// marshal metrics into datapoint structs
	datapoints := make([]graphite.Metric, len(*metrics))
	for i, m := range *metrics {
		datapoints[i] = graphite.Metric{
			Name: m.Id,
			Timestamp: m.Time,
			Value: strconv.FormatFloat(m.Value, 'f', -1, 64),
		}
	}
	return influx.Graphite.SendMetrics(datapoints)
}