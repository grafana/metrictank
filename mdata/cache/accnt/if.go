package accnt

import (
	"github.com/grafana/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

// Accnt represents an instance of cache accounting.
// Currently there is only one implementation called `FlatAccnt`,
// but it could be replaced with alternative eviction algorithms
// in the future if they just implement this interface.
type Accnt interface {
	GetEvictQ() chan *EvictTarget
	AddChunk(metric schema.AMKey, ts uint32, len, cap uint64)
	AddChunks(metric schema.AMKey, chunks []chunk.IterGen)
	HitChunk(metric schema.AMKey, ts uint32)
	HitChunks(metric schema.AMKey, chunks []chunk.IterGen)
	DelMetric(metric schema.AMKey)
	Stop()
	Reset()
}

// EvictTarget is the definition of a chunk that should be evicted.
type EvictTarget struct {
	Metric schema.AMKey
	Ts     uint32
}
