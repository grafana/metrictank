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


// BufferPool33 is a pool that returns cap=33 len=0 byte slices
type BufferPool33 struct {
	pool sync.Pool
}

func NewBufferPool33() *BufferPool33 {
	return &BufferPool33{pool: sync.Pool{
		New: func() interface{} { return make([]byte, 0, 33) },
	}}
}

func (b *BufferPool33) Get() []byte {
	return b.pool.Get().([]byte)
}

func (b *BufferPool33) Put(buf []byte) {
	b.pool.Put(buf[:0])
}
