package stats

import (
	"sync/atomic"
	"time"

	"github.com/Dieterbe/artisanalhistogram/hist15s"
)

// tracks latency measurements in a given range as 32 bit counters
type LatencyHistogram15s32 struct {
	hist  hist15s.Hist15s
	since time.Time
	sum   uint64 // in micros. to generate more accurate mean
	name  []byte
	tags  []byte
}

func NewLatencyHistogram15s32(name string) *LatencyHistogram15s32 {
	return NewLatencyHistogram15s32WithTags(name, "")
}

func NewLatencyHistogram15s32WithTags(name, tags string) *LatencyHistogram15s32 {
	return registry.getOrAdd(name+tags, &LatencyHistogram15s32{
		hist:  hist15s.New(),
		since: time.Now(),
		name:  []byte(name),
		tags:  []byte(tags),
	},
	).(*LatencyHistogram15s32)
}

func (l *LatencyHistogram15s32) Value(t time.Duration) {
	atomic.AddUint64(&l.sum, uint64(t.Nanoseconds()/1000))
	l.hist.AddDuration(t)
}

func (l *LatencyHistogram15s32) WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte {
	snap := l.hist.Snapshot()
	// TODO: once we can actually do cool stuff (e.g. visualize) histogram bucket data, report it
	// for now, only report the summaries :(
	r, ok := l.hist.Report(snap)
	if ok {
		sum := atomic.SwapUint64(&l.sum, 0)
		buf = WriteUint32(buf, prefix, l.name, []byte(".latency.min.gauge32"), l.tags, r.Min/1000, now)
		buf = WriteUint32(buf, prefix, l.name, []byte(".latency.mean.gauge32"), l.tags, uint32((sum / uint64(r.Count) / 1000)), now)
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
