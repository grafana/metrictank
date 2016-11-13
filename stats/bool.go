package stats

import "sync/atomic"

type Bool struct {
	val uint32
}

func NewBool(name string) *Bool {
	return registry.add(name, func() GraphiteMetric {
		return &Bool{}
	}).(*Bool)
}

func (b *Bool) SetTrue() {
	atomic.StoreUint32(&b.val, 1)
}

func (b *Bool) SetFalse() {
	atomic.StoreUint32(&b.val, 0)
}

func (b *Bool) Set(val bool) {
	if val {
		b.SetTrue()
	} else {
		b.SetFalse()
	}
}

func (b *Bool) ReportGraphite(prefix, buf []byte, now int64) []byte {
	val := atomic.LoadUint32(&b.val)
	buf = WriteUint32(buf, prefix, nil, val, now)
	return buf
}
