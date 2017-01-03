package stats

import (
	"sync/atomic"
	"time"
)

type Gauge32 struct {
	val uint32
}

func NewGauge32(name string) *Gauge32 {
	return registry.getOrAdd(name, &Gauge32{}).(*Gauge32)
}

func (g *Gauge32) Inc() {
	atomic.AddUint32(&g.val, 1)
}

func (g *Gauge32) Dec() {
	atomic.AddUint32(&g.val, ^uint32(0))
}

func (g *Gauge32) AddUint32(val uint32) {
	atomic.AddUint32(&g.val, val)
}

func (g *Gauge32) DecUint32(val uint32) {
	atomic.AddUint32(&g.val, ^uint32(val-1))
}

func (g *Gauge32) Add(val int) {
	if val == 0 {
		return
	}
	if val > 0 {
		g.AddUint32(uint32(val))
		return
	}
	// < 0
	g.DecUint32(uint32(-1 * val))
}

func (g *Gauge32) Set(val int) {
	atomic.StoreUint32(&g.val, uint32(val))
}

func (g *Gauge32) SetUint32(val uint32) {
	atomic.StoreUint32(&g.val, val)
}

func (g *Gauge32) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	val := atomic.LoadUint32(&g.val)
	buf = WriteUint32(buf, prefix, []byte("gauge32"), val, now)
	return buf
}
