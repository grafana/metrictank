package stats

import (
	"sync/atomic"
	"time"
)

type Counter32 struct {
	val  uint32
	name []byte
	tags []byte
}

func NewCounter32(name string) *Counter32 {
	return NewCounter32WithTags(name, "")
}

func NewCounter32WithTags(name, tags string) *Counter32 {
	return registry.getOrAdd(name+tags, &Counter32{
		name: []byte(name),
		tags: []byte(tags),
	}).(*Counter32)
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

func (c *Counter32) WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte {
	val := atomic.LoadUint32(&c.val)
	buf = WriteUint32(buf, prefix, c.name, []byte(".counter32"), c.tags, val, now)
	return buf
}
