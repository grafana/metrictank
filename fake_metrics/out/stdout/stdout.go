package stdout

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/schema"
)

var (
	metricsPublished met.Count
	publishDuration  met.Timer
)

type Stdout struct {
	sync.Mutex
}

func New(stats met.Backend) *Stdout {

	metricsPublished = stats.NewCount("metricpublisher.stdout.metrics-published")
	publishDuration = stats.NewTimer("metricpublisher.stdout.publish_duration", 0)

	return &Stdout{
		sync.Mutex{},
	}
}

func (s *Stdout) Close() error {
	return nil
}

func (s *Stdout) Publish(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}
	pre := time.Now()
	s.Lock()
	for _, m := range metrics {
		_, err := fmt.Fprintf(os.Stdout, "org_%d.%s %f %d\n", m.OrgId, m.Name, m.Value, m.Time)
		if err != nil {
			return err
		}
	}
	s.Unlock()
	publishDuration.Value(time.Since(pre))
	metricsPublished.Inc(int64(len(metrics)))
	return nil
}
