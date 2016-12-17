package cache

// Flat accounting

type FlatAccnt struct {
	total   uint64
	metrics map[string]*FlatAccntMet
	maxSize uint64
	lru     *LRU
	evictQ  chan *EvictTarget
	eventQ  chan *FlatAccntEvent
}

type FlatAccntMet struct {
	total  uint64
	chunks map[uint32]uint64
}

// event types to be used in FlatAccntEvent
const evnt_add uint8 = 0
const evnt_hit uint8 = 1

type FlatAccntEvent struct {
	t      uint8 // event type
	metric string
	ts     uint32
	size   uint64
}

func (a *FlatAccnt) init() {
	a.eventQ = make(chan *FlatAccntEvent)
	go a.eventLoop()
}

func (a *FlatAccnt) Add(metric string, ts uint32, size uint64) {
	a.act(evnt_add, metric, ts, size)
}

func (a *FlatAccnt) Hit(metric string, ts uint32) {
	a.act(evnt_hit, metric, ts, 0)
}

func (a *FlatAccnt) act(t uint8, metric string, ts uint32, size uint64) {
	a.eventQ <- &FlatAccntEvent{
		t:      t,
		metric: metric,
		ts:     ts,
		size:   size,
	}
}

func (a *FlatAccnt) eventLoop() {
	for {
		event := <-a.eventQ
		if event.t == evnt_add {
			a.add(event.metric, event.ts, event.size)
		}

		a.lru.touch(
			EvictTarget{
				Metric: event.metric,
				Ts:     event.ts,
			},
		)

		// evict until we're below the max
		for {
			if a.total <= a.maxSize {
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
	target := a.lru.pop().(EvictTarget)
	a.evictQ <- &target
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
	met.total = met.total - size
	a.total = a.total - size

	if met.total <= 0 {
		delete(a.metrics, metric)
	}
}

func (a *FlatAccnt) GetEvictQ() chan *EvictTarget {
	return a.evictQ
}
