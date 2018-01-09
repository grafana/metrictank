package accnt

// Accnt represents an instance of cache accounting.
// Currently there is only one implementation called `FlatAccnt`,
// but it could be replaced with alternative eviction algorithms
// in the future if they just implement this interface.
type Accnt interface {
	GetEvictQ() chan *EvictTarget
	AddChunk(metric string, ts uint32, size uint64)
	HitChunk(metric string, ts uint32)
	DelMetric(metric string)
	Stop()
	Reset()
}

// EvictTarget is the definition of a chunk that should be evicted.
type EvictTarget struct {
	Metric string
	Ts     uint32
}
