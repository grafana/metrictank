package util

import (
	"sync"
)

type BufferPool struct {
	pool sync.Pool
}

func NewBufferPool() *BufferPool {
	return &BufferPool{pool: sync.Pool{
		New: func() interface{} { return make([]byte, 0) },
	}}
}

func (b *BufferPool) Get() []byte {
	return b.pool.Get().([]byte)
}

func (b *BufferPool) Put(buf []byte) {
	b.pool.Put(buf[:0])
}
