package runner

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	vegeta "github.com/tsenart/vegeta/lib"
)

type Stat struct {
	name  string
	count int
	lat   vegeta.LatencyMetrics
}

func NewStat(name string) Stat {
	return Stat{
		name: name,
	}
}

func (s *Stat) Add(dur time.Duration) {
	s.count++
	s.lat.Add(dur)
}

func (s Stat) Report() {
	mean := time.Duration(float64(s.lat.Total) / float64(s.count))
	p50 := s.lat.Quantile(0.50)
	p95 := s.lat.Quantile(0.95)
	p99 := s.lat.Quantile(0.99)

	const fmtstr = "Name\t%s\n" +
		"Requests\t[total]\t%d\n" +
		"Latencies\t[mean, 50, 95, 99, max]\t%s, %s, %s, %s, %s\n"

	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', tabwriter.StripEscape)
	_, err := fmt.Fprintf(tw, fmtstr,
		s.name,
		s.count,
		mean, p50, p95, p99, s.lat.Max,
	)
	if err != nil {
		panic(err)
	}

	err = tw.Flush()

	if err != nil {
		panic(err)
	}
}
