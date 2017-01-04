package cache

import (
	"github.com/raintank/metrictank/mdata/chunk"
)

type CCacheChunk struct {
	// the timestamp of this cache chunk
	Ts uint32

	// the previous chunk. if the previous chunk isn't cached this is 0
	Prev uint32

	// the next chunk. if the next chunk isn't cached this is 0
	Next uint32

	// an iterator generator to iterate over the chunk's data
	Itgen chunk.IterGen
}
