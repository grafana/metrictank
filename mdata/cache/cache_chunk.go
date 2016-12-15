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
