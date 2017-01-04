package mdata

import (
	"github.com/raintank/metrictank/mdata/chunk"
)

type Store interface {
	Add(cwr *ChunkWriteRequest)
	Search(key string, start, end uint32) ([]chunk.IterGen, error)
	Stop()
}
