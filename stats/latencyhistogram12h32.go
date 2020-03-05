package stats

import (
	"time"

	"github.com/Dieterbe/artisanalhistogram/hist12h"
)

// tracks latency measurements in a given range as 32 bit counters
type LatencyHistogram12h32 struct {
	hist  hist12h.Hist12h
	since time.Time
	name  []byte
	tags  []byte
}

func NewLatencyHistogram12h32(name string) *LatencyHistogram12h32 {
	return registry.getOrAdd(name, &LatencyHistogram12h32{
		hist:  hist12h.New(),
		since: time.Now(),
		name:  []byte(name),
	},
	).(*LatencyHistogram12h32)
}

func (l *LatencyHistogram12h32) Value(t time.Duration) {
	l.hist.AddDuration(t)
}

func (l *LatencyHistogram12h32) WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte {
	snap := l.hist.Snapshot()
	// TODO: once we can actually do cool stuff (e.g. visualize) histogram bucket data, report it
	// for now, only report the summaries :(
	r, ok := l.hist.Report(snap)
	if ok {
		buf = WriteUint32(buf, prefix, l.name, []byte(".latency.min.gauge32"), l.tags, r.Min/1000, now)
		buf = WriteUint32(buf, prefix, l.name, []byte(".latency.mean.gauge32"), l.tags, r.Mean/1000, now)
		buf = WriteUint32(buf, prefix, l.name, []byte(".latency.median.gauge32"), l.tags, r.Median/1000, now)
		buf = WriteUint32(buf, prefix, l.name, []byte(".latency.p75.gauge32"), l.tags, r.P75/1000, now)
		buf = WriteUint32(buf, prefix, l.name, []byte(".latency.p90.gauge32"), l.tags, r.P90/1000, now)
		buf = WriteUint32(buf, prefix, l.name, []byte(".latency.max.gauge32"), l.tags, r.Max/1000, now)
	}
	buf = WriteUint32(buf, prefix, l.name, []byte(".values.count32"), l.tags, r.Count, now)
	buf = WriteFloat64(buf, prefix, l.name, []byte(".values.rate32"), l.tags, float64(r.Count)/now.Sub(l.since).Seconds(), now)
	l.since = now
	return buf
}
