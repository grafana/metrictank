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
}

// EvictTarget is the definition of a chunk that should be evicted.
type EvictTarget struct {
	Metric string
	Ts     uint32
}

type Stats struct {
	// metric full hits, all requested chunks were cached
	complt_met uint32

	// metric complete misses, not a single chunk of the request was cached
	miss_met uint32

	// metric partial hits, some of the requested chunks were cached
	part_met uint32

	// new metrics added
	add_met uint32

	// metrics completely evicted
	evict_met uint32

	// chunk hits
	hit_chnk uint32

	// chunk adds
	add_chnk uint32

	// chunk evictions
	evict_chnk uint32
}
