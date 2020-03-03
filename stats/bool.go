package stats

import (
	"sync/atomic"
	"time"
)

type Bool struct {
	val  uint32
	name []byte
	tags []byte
}

func NewBool(name, tags string) *Bool {
	return registry.getOrAdd(name+tags, &Bool{
		name: []byte(name),
		tags: []byte(tags),
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

func (b *Bool) Peek() bool {
	if atomic.LoadUint32(&b.val) == 0 {
		return false
	}
	return true
}

func (b *Bool) WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte {
	val := atomic.LoadUint32(&b.val)
	buf = WriteUint32(buf, prefix, b.name, []byte(".gauge1"), b.tags, val, now)
	return buf
}
