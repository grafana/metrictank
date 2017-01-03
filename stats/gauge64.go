package stats

import (
	"sync/atomic"
	"time"
)

type Gauge64 struct {
	val uint64
}

func NewGauge64(name string) *Gauge64 {
	return registry.getOrAdd(name, &Gauge64{}).(*Gauge64)
}

func (g *Gauge64) Inc() {
	atomic.AddUint64(&g.val, 1)
}

func (g *Gauge64) Dec() {
	atomic.AddUint64(&g.val, ^uint64(0))
}

func (g *Gauge64) AddUint64(val uint64) {
	atomic.AddUint64(&g.val, val)
}

func (g *Gauge64) DecUint64(val uint64) {
	atomic.AddUint64(&g.val, ^uint64(val-1))
}

func (g *Gauge64) Add(val int) {
	if val == 0 {
		return
	}
	if val > 0 {
		g.AddUint64(uint64(val))
		return
	}
	// < 0
	g.DecUint64(uint64(-1 * val))
}

func (g *Gauge64) Set(val int) {
	atomic.StoreUint64(&g.val, uint64(val))
}

func (g *Gauge64) SetUint64(val uint64) {
	atomic.StoreUint64(&g.val, val)
}

func (g *Gauge64) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	val := atomic.LoadUint64(&g.val)
	buf = WriteUint64(buf, prefix, []byte("gauge64"), val, now)
	return buf
}
