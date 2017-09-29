package mdata

import (
	"context"

	"github.com/grafana/metrictank/mdata/chunk"
)

type Store interface {
	Add(cwr *ChunkWriteRequest)
	Search(ctx context.Context, key string, ttl, start, end uint32) ([]chunk.IterGen, error)
	Stop()
}
