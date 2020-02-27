package stats

import (
	"sync/atomic"
	"time"
)

type Bool struct {
	val uint32
}

func NewBool(name string) *Bool {
	return registry.getOrAdd(name, &Bool{}).(*Bool)
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

func (b *Bool) Peek() bool {
	if atomic.LoadUint32(&b.val) == 0 {
		return false
	}
	return true
}

func (b *Bool) WriteGraphiteLine(buf, prefix, name, tags []byte, now time.Time) []byte {
	val := atomic.LoadUint32(&b.val)
	buf = WriteUint32(buf, prefix, name, []byte(".gauge1"), tags, val, now)
	return buf
}
