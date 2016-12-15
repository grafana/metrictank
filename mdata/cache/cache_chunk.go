package cache

import (
	"github.com/raintank/metrictank/iter"
)

type CacheChunk struct {
	Ts    uint32
	Next  uint32
	Prev  uint32
	Itgen iter.IterGen
}

// this assumes we have a lock
func (cc *CacheChunk) setNext(next uint32) {
	cc.Next = next
}

// this assumes we have a lock
func (cc *CacheChunk) setPrev(prev uint32) {
	cc.Prev = prev
}
