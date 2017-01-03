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

func (b *Bool) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	val := atomic.LoadUint32(&b.val)
	buf = WriteUint32(buf, prefix, []byte("gauge1"), val, now)
	return buf
}
