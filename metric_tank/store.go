package main

import (
	"github.com/raintank/raintank-metric/metric_tank/iter"
)

type Store interface {
	Add(cwr *ChunkWriteRequest)
	Search(key string, start, end uint32) ([]iter.Iter, error)
	Stop()
}
