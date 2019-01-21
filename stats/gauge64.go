package stats

import (
	"sync/atomic"
	"time"
)

type Gauge64 uint64

func NewGauge64(name string) *Gauge64 {
	u := Gauge64(0)
	return registry.getOrAdd(name, &u).(*Gauge64)
}

func (g *Gauge64) Inc() {
	atomic.AddUint64((*uint64)(g), 1)
}

func (g *Gauge64) Dec() {
	atomic.AddUint64((*uint64)(g), ^uint64(0))
}

func (g *Gauge64) AddUint64(val uint64) {
	atomic.AddUint64((*uint64)(g), val)
}

func (g *Gauge64) DecUint64(val uint64) {
	atomic.AddUint64((*uint64)(g), ^uint64(val-1))
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
	atomic.StoreUint64((*uint64)(g), uint64(val))
}

func (g *Gauge64) SetUint64(val uint64) {
	atomic.StoreUint64((*uint64)(g), val)
}

func (g *Gauge64) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	val := atomic.LoadUint64((*uint64)(g))
	buf = WriteUint64(buf, prefix, []byte("gauge64"), val, now)
	return buf
}

func (g *Gauge64) Peek() uint64 {
	return atomic.LoadUint64((*uint64)(g))
}
