package cache

import (
	"github.com/raintank/metrictank/iter"
)

type Cache interface {
	Add(string, uint32, iter.IterGen) error
}
