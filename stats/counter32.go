package stats

import (
	"sync/atomic"
	"time"
)

type Counter32 struct {
	val uint32
}

func NewCounter32(name string) *Counter32 {
	return registry.getOrAdd(name, &Counter32{}).(*Counter32)
}

func (c *Counter32) SetUint32(val uint32) {
	atomic.StoreUint32(&c.val, val)
}

func (c *Counter32) Inc() {
	atomic.AddUint32(&c.val, 1)
}

func (c *Counter32) Add(val int) {
	c.AddUint32(uint32(val))
}

func (c *Counter32) AddUint32(val uint32) {
	atomic.AddUint32(&c.val, val)
}

func (c *Counter32) Peek() uint32 {
	return atomic.LoadUint32(&c.val)
}

func (c *Counter32) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	val := atomic.LoadUint32(&c.val)
	buf = WriteUint32(buf, prefix, []byte("counter32"), val, now)
	return buf
}
