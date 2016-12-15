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
