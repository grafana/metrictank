package stats

import (
	"sync/atomic"
	"time"
)

type Gauge64 struct {
	val  uint64
	name []byte
	tags []byte
}

func NewGauge64(name string) *Gauge64 {
	return NewGauge64WithTags(name, "")
}

func NewGauge64WithTags(name, tags string) *Gauge64 {
	return registry.getOrAdd(name+tags, &Gauge64{
		name: []byte(name),
		tags: []byte(tags),
	}).(*Gauge64)
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

func (g *Gauge64) WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte {
	val := atomic.LoadUint64(&g.val)
	buf = WriteUint64(buf, prefix, g.name, []byte(".gauge64"), g.tags, val, now)
	return buf
}

func (g *Gauge64) Peek() uint64 {
	return atomic.LoadUint64(&g.val)
}
