package stats

import (
	"time"

	"github.com/Dieterbe/artisanalhistogram/hist12h"
)

// tracks latency measurements in a given range as 32 bit counters
type LatencyHistogram12h32 struct {
	hist hist12h.Hist12h
}

func NewLatencyHistogram12h32(name string) *LatencyHistogram12h32 {
	return registry.add(name, func() GraphiteMetric {
		return &LatencyHistogram12h32{
			hist: hist12h.New(),
		}
	}).(*LatencyHistogram12h32)
}

func (l *LatencyHistogram12h32) Value(t time.Duration) {
	l.hist.AddDuration(t)
}

func (l *LatencyHistogram12h32) ReportGraphite(prefix, buf []byte, now int64) []byte {
	snap := l.hist.Snapshot()
	// TODO: once we can actually do cool stuff (e.g. visualize) histogram bucket data, report it
	// for now, only report the summaries :(
	r, ok := l.hist.Report(snap)
	if ok {
		buf = WriteUint32(buf, prefix, []byte("min"), r.Min/1000, now)
		buf = WriteUint32(buf, prefix, []byte("mean"), r.Mean/1000, now)
		buf = WriteUint32(buf, prefix, []byte("median"), r.Median/1000, now)
		buf = WriteUint32(buf, prefix, []byte("p75"), r.P75/1000, now)
		buf = WriteUint32(buf, prefix, []byte("p90"), r.P90/1000, now)
		buf = WriteUint32(buf, prefix, []byte("max"), r.Max/1000, now)
		buf = WriteUint32(buf, prefix, []byte("count"), r.Count, now)
	}
	return buf
}
