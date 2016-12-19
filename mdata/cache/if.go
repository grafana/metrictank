package cache

import (
	"github.com/raintank/metrictank/mdata/chunk"
)

type Cache interface {
	Add(string, uint32, chunk.IterGen) error
	Search(string, uint32, uint32) *CCSearchResult
}
