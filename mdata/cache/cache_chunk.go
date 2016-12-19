package cache

import (
	"github.com/raintank/metrictank/mdata/chunk"
)

type CacheChunk struct {
	Ts    uint32
	Prev  uint32
	Next  uint32
	Itgen chunk.IterGen
}
