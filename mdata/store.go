package mdata

import (
	"github.com/raintank/metrictank/iter"
)

type Store interface {
	Add(cwr *ChunkWriteRequest)
	Search(key string, start, end uint32) ([]iter.IterGen, error)
	Stop()
}
