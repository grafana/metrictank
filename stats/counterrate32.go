package stats

import (
	"sync/atomic"
	"time"
)

// CounterRate32 publishes a counter32 as well as a rate32 in seconds
type CounterRate32 struct {
	prev  uint32
	val   uint32
	since time.Time
	name  []byte
	tags  []byte
}

func NewCounterRate32(name string) *CounterRate32 {
	return NewTaggedCounterRate32(name, "")
}

func NewTaggedCounterRate32(name, tags string) *CounterRate32 {
	return registry.getOrAdd(name+tags, &CounterRate32{
		since: time.Now(),
		name:  []byte(name),
		tags:  []byte(tags),
	}).(*CounterRate32)
}

func (c *CounterRate32) SetUint32(val uint32) {
	atomic.StoreUint32(&c.val, val)
}

func (c *CounterRate32) Inc() {
	atomic.AddUint32(&c.val, 1)
}

func (c *CounterRate32) Add(val int) {
	c.AddUint32(uint32(val))
}

func (c *CounterRate32) AddUint32(val uint32) {
	atomic.AddUint32(&c.val, val)
}

func (c *CounterRate32) Peek() uint32 {
	return atomic.LoadUint32(&c.val)
}

func (c *CounterRate32) WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte {
	val := atomic.LoadUint32(&c.val)
	buf = WriteUint32(buf, prefix, c.name, []byte(".counter32"), c.tags, val, now)
	buf = WriteFloat64(buf, prefix, c.name, []byte(".rate32"), c.tags, float64(val-c.prev)/now.Sub(c.since).Seconds(), now)

	c.prev = val
	c.since = now
	return buf
}
