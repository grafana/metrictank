package accnt

import "gopkg.in/raintank/schema.v1"

// Accnt represents an instance of cache accounting.
// Currently there is only one implementation called `FlatAccnt`,
// but it could be replaced with alternative eviction algorithms
// in the future if they just implement this interface.
type Accnt interface {
	GetEvictQ() chan *EvictTarget
	AddChunk(metric schema.AMKey, ts uint32, size uint64)
	HitChunk(metric schema.AMKey, ts uint32)
	DelMetric(metric schema.AMKey)
	Stop()
	Reset()
}

// EvictTarget is the definition of a chunk that should be evicted.
type EvictTarget struct {
	Metric schema.AMKey
	Ts     uint32
}
