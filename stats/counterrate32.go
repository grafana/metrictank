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
}

func NewCounterRate32(name string) *CounterRate32 {
	c := CounterRate32{
		since: time.Now(),
	}
	return registry.getOrAdd(name, &c).(*CounterRate32)
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

func (c *CounterRate32) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	val := atomic.LoadUint32(&c.val)
	buf = WriteUint32(buf, prefix, []byte("counter32"), val, now)
	buf = WriteFloat64(buf, prefix, []byte("rate32"), float64(val-c.prev)/now.Sub(c.since).Seconds(), now)

	c.prev = val
	c.since = now
	return buf
}
