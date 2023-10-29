package publish

import (
	"strconv"

	schema "github.com/grafana/metrictank/internal/schema"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	//Publish the given metrics.
	//Requires that the metric interval and ID have been set
	Publish(metrics []*schema.MetricData) error
	//Type returns the type of system the Publisher publishes to
	Type() string
}

var (
	publisher Publisher
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
