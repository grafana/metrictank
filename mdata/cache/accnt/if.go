package cache

type Accnt interface {
	Add(string, uint32, uint64)
	Hit(string, uint32)
	GetEvictQ() chan *EvictTarget
}

type EvictTarget struct {
	Metric string
	Ts     uint32
}

func NewAccnt(maxSize uint64) Accnt {
	accnt := &FlatAccnt{
		total:   0,
		metrics: make(map[string]*FlatAccntMet),
		maxSize: maxSize,
		lru:     NewLRU(),
		evictQ:  make(chan *EvictTarget),
	}
	accnt.init()
	return accnt
}
