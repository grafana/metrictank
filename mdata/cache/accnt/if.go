package accnt

// Accnt represents an instance of cache accounting.
// Currently there is only one implementation called `FlatAccnt`,
// but it could be replaced with alternative eviction algorithms
// in the future if they just implement this interface.
type Accnt interface {
	GetEvictQ() chan *EvictTarget
	AddChunk(string, uint32, uint64)
	HitChunk(string, uint32)

	// these methods are for stats only
	MissMetric()
	PartialMetric()
	CompleteMetric()
}

// used by accounting to tell the chunk cache what to evict
type EvictTarget struct {
	Metric string
	Ts     uint32
}

func NewAccnt(maxSize uint64) Accnt {
	// currently we only know NewFlatAccnt
	return NewFlatAccnt(maxSize)
}
