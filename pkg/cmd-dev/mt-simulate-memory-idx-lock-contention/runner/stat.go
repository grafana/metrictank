package runner

import (
	"fmt"
	"os"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/spenczar/tdigest"
)

type Stat struct {
	Name  string
	Count int
	Total time.Duration
	Max   time.Duration
	td    *tdigest.TDigest
	mut   *sync.Mutex
}

func NewStat(name string) *Stat {
	return &Stat{
		Name: name,
		td:   tdigest.New(),
		mut:  &sync.Mutex{},
	}
}

func (s *Stat) Add(dur time.Duration) {
	s.mut.Lock()
	s.Count++
	s.Total += dur
	if dur > s.Max {
		s.Max = dur
	}
	s.td.Add(float64(dur), 1)
	s.mut.Unlock()
}

func (s Stat) Report() {
	s.mut.Lock()
	mean := time.Duration(float64(s.Total) / float64(s.Count))
	p50 := time.Duration(s.td.Quantile(0.50))
	p95 := time.Duration(s.td.Quantile(0.95))
	p99 := time.Duration(s.td.Quantile(0.99))

	const fmtstr = "Name\t%s\n" +
		"Requests\t[total]\t%d\n" +
		"Latencies\t[mean, 50, 95, 99, max]\t%s, %s, %s, %s, %s\n"

	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', tabwriter.StripEscape)
	_, err := fmt.Fprintf(tw, fmtstr,
		s.Name,
		s.Count,
		mean, p50, p95, p99, s.Max,
	)
	if err != nil {
		panic(err)
	}

	err = tw.Flush()

	if err != nil {
		panic(err)
	}
	s.mut.Unlock()
}

// ReportComputer reports the stat in a computer friendly form in microseconds.
func (s Stat) ReportComputer() {
	s.mut.Lock()
	mean := int64(s.Total) / int64(s.Count) / 1e3
	p50 := s.td.Quantile(0.50) / 1e3
	p95 := s.td.Quantile(0.95) / 1e3
	p99 := s.td.Quantile(0.99) / 1e3
	fmt.Println(s.Name, s.Count, mean, p50, p95, p99, s.Max.Nanoseconds()/1e3)
	s.mut.Unlock()
}
