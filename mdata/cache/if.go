package cache

import (
	"github.com/raintank/metrictank/mdata/chunk"
)

type Cache interface {
	Add(string, uint32, chunk.IterGen)
	Search(string, uint32, uint32) *CCSearchResult
}
