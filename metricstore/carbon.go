package metricstore

import (
	"github.com/marpaia/graphite-golang"
	"github.com/raintank/raintank-metric/metricdef"
	"strconv"
)

// Kairosdb client
type Carbon struct {
	Graphite *graphite.Graphite
}

func NewCarbon(host string, port int) (*Carbon, error) {
	graphite, err := graphite.NewGraphite(host, port)
	if err != nil {
		return nil, err
	}
	return &Carbon{Graphite: graphite}, nil
}

func (carbon *Carbon) SendMetrics(metrics *[]metricdef.IndvMetric) error {
	// marshal metrics into datapoint structs
	datapoints := make([]graphite.Metric, len(*metrics))
	for i, m := range *metrics {
		datapoints[i] = graphite.Metric{
			Name:      m.Id,
			Timestamp: m.Time,
			Value:     strconv.FormatFloat(m.Value, 'f', -1, 64),
		}
	}
	return carbon.Graphite.SendMetrics(datapoints)
}

func (carbon *Carbon) Type() string {
	return "Carbon"
}
