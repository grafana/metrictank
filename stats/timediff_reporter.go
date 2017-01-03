package stats

import (
	"sync/atomic"
	"time"
)

// reports the time in seconds until a specific timestamp is reached
// once reached, reports 0
type TimeDiffReporter32 struct {
	target uint32
}

func NewTimeDiffReporter32(name string, target uint32) *TimeDiffReporter32 {
	return registry.getOrAdd(name, &TimeDiffReporter32{
		target: target,
	},
	).(*TimeDiffReporter32)
}

func (g *TimeDiffReporter32) Set(target uint32) {
	atomic.StoreUint32(&g.target, target)
}

func (g *TimeDiffReporter32) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	target := atomic.LoadUint32(&g.target)
	now32 := uint32(now.Unix())
	report := uint32(0)
	if now32 < target {
		report = target - now32
	}
	buf = WriteUint32(buf, prefix, []byte("gauge32"), report, now)
	return buf
}
