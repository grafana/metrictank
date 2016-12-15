package cache

// Flat accounting

type FlatAccnt struct {
	total   uint64
	metrics map[string]*FlatAccntMet
	maxSize uint64
	lru     *LRU
	evictQ  chan *EvictTarget
	actionQ chan *FlatAccntAction
}

type FlatAccntMet struct {
	total  uint64
	chunks map[uint32]uint64
}

const (
	// action types to be used in FlatAccntAction
	hrc_add = iota
	hrc_hit = iota
)

type FlatAccntAction struct {
	t      int // type
	metric string
	ts     uint32
	size   uint64
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

func (a *FlatAccnt) init() {
	a.actionQ = make(chan *FlatAccntAction)
	go a.actionLoop()
}

func (a *FlatAccnt) Add(metric string, ts uint32, size uint64) {
	a.act(hrc_add, metric, ts, size)
}

func (a *FlatAccnt) Hit(metric string, ts uint32) {
	a.act(hrc_hit, metric, ts, 0)
}

func (a *FlatAccnt) act(t int, metric string, ts uint32, size uint64) {
	a.actionQ <- &FlatAccntAction{
		t:      t,
		metric: metric,
		ts:     ts,
		size:   0,
	}
}

func (a *FlatAccnt) actionLoop() {
	for action := range a.actionQ {
		if action.t == hrc_add {
			a.add(action.metric, action.ts, action.size)
		}

		a.lru.touch(
			&EvictTarget{
				Metric: action.metric,
				Ts:     action.ts,
			},
		)

		// evict until we're below the max
		for {
			if a.total < a.maxSize {
				break
			}
			a.evict()
		}
	}
}

func (a *FlatAccnt) add(metric string, ts uint32, size uint64) {
	var met *FlatAccntMet
	var ok bool

	if met, ok = a.metrics[metric]; !ok {
		met = &FlatAccntMet{
			total:  0,
			chunks: make(map[uint32]uint64),
		}
		a.metrics[metric] = met
	}

	if _, ok = met.chunks[ts]; ok {
		// we already have that chunk
		return
	}

	met.chunks[ts] = size
	met.total = met.total + size
	a.total = a.total + size
}

func (a *FlatAccnt) evict() {
	target := a.lru.pop().(*EvictTarget)
	a.evictQ <- target
	a.del(target.Metric, target.Ts)
}

func (a *FlatAccnt) del(metric string, ts uint32) {
	var met *FlatAccntMet
	var ok bool
	var size uint64

	if met, ok = a.metrics[metric]; !ok {
		// we don't have this metric
		return
	}

	if size, ok = met.chunks[ts]; !ok {
		// we don't have that chunk
		return
	}

	delete(met.chunks, ts)
	a.total = a.total - size

	if len(met.chunks) == 0 {
		delete(a.metrics, metric)
	}
}

func (a *FlatAccnt) GetEvictQ() chan *EvictTarget {
	return a.evictQ
}
