package stats

import (
	"time"

	"github.com/Dieterbe/artisanalhistogram/hist15s"
)

// tracks latency measurements in a given range as 32 bit counters
type LatencyHistogram15s32 struct {
	hist hist15s.Hist15s
}

func NewLatencyHistogram15s32(name string) *LatencyHistogram15s32 {
	return registry.add(name, func() GraphiteMetric {
		return &LatencyHistogram15s32{
			hist: hist15s.New(),
		}
	}).(*LatencyHistogram15s32)
}

func (l *LatencyHistogram15s32) Value(t time.Duration) {
	l.hist.AddDuration(t)
}

func (l *LatencyHistogram15s32) ReportGraphite(prefix, buf []byte, now int64) []byte {
	snap := l.hist.Snapshot()
	// TODO: once we can actually do cool stuff (e.g. visualize) histogram bucket data, report it
	// for now, only report the summaries :(
	r, ok := l.hist.Report(snap)
	if ok {
		buf = WriteUint32(buf, prefix, []byte("min.gauge32"), r.Min/1000, now)
		buf = WriteUint32(buf, prefix, []byte("mean.gauge32"), r.Mean/1000, now)
		buf = WriteUint32(buf, prefix, []byte("median.gauge32"), r.Median/1000, now)
		buf = WriteUint32(buf, prefix, []byte("p75.gauge32"), r.P75/1000, now)
		buf = WriteUint32(buf, prefix, []byte("p90.gauge32"), r.P90/1000, now)
		buf = WriteUint32(buf, prefix, []byte("max.gauge32"), r.Max/1000, now)
		buf = WriteUint32(buf, prefix, []byte("values.count32"), r.Count, now)
	}
	return buf
}
