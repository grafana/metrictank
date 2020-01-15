package publish

import (
	"strconv"

	schema "github.com/grafana/metrictank/schema"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/raintank/tsdb-gw/metrics_client"
	log "github.com/sirupsen/logrus"
)

var (
	ingestedMetrics = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "samples_ingested_total",
		Help:      "Number of samples ingested",
	}, []string{"org"})
)

type Publisher interface {
	Publish(metrics []*schema.MetricData) error
	Type() string
}

var (
	publisher Publisher

	// Persister allows pushing metrics to the Persistor Service
	Persistor *metrics_client.Client
)

func Init(p Publisher) {
	if p == nil {
		publisher = &nullPublisher{}
	} else {
		publisher = p
	}
	log.Infof("using %s publisher", publisher.Type())
}

func Publish(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}

	if err := publisher.Publish(metrics); err != nil {
		return err
	}
	// capture accounting data.
	orgCounts := make(map[int]int32)
	for _, m := range metrics {
		orgCounts[m.OrgId]++
	}
	for org, count := range orgCounts {
		ingestedMetrics.WithLabelValues(strconv.Itoa(org)).Add(float64(count))
	}
	return nil
}

// nullPublisher drops all metrics passed through the publish interface
type nullPublisher struct{}

func (*nullPublisher) Publish(metrics []*schema.MetricData) error {
	log.Debugf("publishing not enabled, dropping %d metrics", len(metrics))
	return nil
}

func (*nullPublisher) Type() string {
	return "nullPublisher"
}

func Persist(metrics []*schema.MetricData) error {
	return publisher.Publish(metrics)
}
