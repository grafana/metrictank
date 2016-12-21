package accnt

// represents an instance of cache accounting. currently there is
// only one implementation called `FlatAccnt`, but it could be
// replaced with alternative eviction algorithms in the future if
// they just implement this interface
type Accnt interface {
	AddChunk(string, uint32, uint64)
	HitChunk(string, uint32)
	GetEvictQ() chan *EvictTarget

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
