package stdout

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/grafana/metrictank/schema"
	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/met"
)

type Stdout struct {
	sync.Mutex
	out.OutStats
}

func New(stats met.Backend) *Stdout {
	return &Stdout{
		sync.Mutex{},
		out.NewStats(stats, "stdout"),
	}
}

func (s *Stdout) Close() error {
	return nil
}

func (s *Stdout) Flush(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		s.FlushDuration.Value(0)
		return nil
	}
	preFlush := time.Now()
	prePub := time.Now()
	var n int64
	s.Lock()
	for _, m := range metrics {
		num, err := fmt.Fprintf(os.Stdout, "%d %s (tags: %v) %f %d\n", m.OrgId, m.Name, m.Tags, m.Value, m.Time)
		if err != nil {
			s.PublishErrors.Inc(1)
			return err
		}
		n += int64(num)
	}
	s.MessageBytes.Value(n)
	s.MessageMetrics.Value(int64(len(metrics)))
	s.Unlock()
	s.PublishedMetrics.Inc(int64(len(metrics)))
	s.PublishedMessages.Inc(1)
	s.PublishDuration.Value(time.Since(prePub))
	s.FlushDuration.Value(time.Since(preFlush))
	return nil
}
