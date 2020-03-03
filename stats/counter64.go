package stats

import (
	"sync/atomic"
	"time"
)

type Counter64 struct {
	val  uint64
	name []byte
	tags []byte
}

func NewCounter64(name string) *Counter64 {
	return registry.getOrAdd(name, &Counter64{
		name: []byte(name),
	}).(*Counter64)
}

func (c *Counter64) SetUint64(val uint64) {
	atomic.StoreUint64(&c.val, val)
}

func (c *Counter64) Inc() {
	atomic.AddUint64(&c.val, 1)
}

func (c *Counter64) AddUint64(val uint64) {
	atomic.AddUint64(&c.val, val)
}

func (c *Counter64) WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte {
	val := atomic.LoadUint64(&c.val)
	buf = WriteUint64(buf, prefix, c.name, []byte(".counter64"), c.tags, val, now)
	return buf
}
