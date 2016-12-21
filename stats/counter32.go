package stats

import "sync/atomic"

type Counter32 struct {
	val uint32
}

func NewCounter32(name string) *Counter32 {
	return registry.add(name, func() GraphiteMetric {
		return &Counter32{}
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

func (c *Counter32) ReportGraphite(prefix, buf []byte, now int64) []byte {
	val := atomic.LoadUint32(&c.val)
	buf = WriteUint32(buf, prefix, []byte("counter32"), val, now)
	return buf
}
