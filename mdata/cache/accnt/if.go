package accnt

// Accnt represents an instance of cache accounting.
// Currently there is only one implementation called `FlatAccnt`,
// but it could be replaced with alternative eviction algorithms
// in the future if they just implement this interface.
type Accnt interface {
	GetEvictQ() chan *EvictTarget
	AddChunk(string, uint32, uint64)
	HitChunk(string, uint32)
	Stop()
	Reset()
}

// EvictTarget is the definition of a chunk that should be evicted.
type EvictTarget struct {
	Metric string
	Ts     uint32
}
