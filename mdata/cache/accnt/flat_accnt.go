package accnt

import (
	"sort"
)

// Flat accounting
//
// Keeps track of the chunk cache size and in which order the contained
// chunks have been used to last time. If it detects that the total cache
// size is above the given limit, it feeds the least recently used
// cache chunks into the evict queue, which will get consumed by the
// evict loop.

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

func NewFlatAccnt(maxSize uint64) Accnt {
	accnt := &FlatAccnt{
		total:   0,
		metrics: make(map[string]*FlatAccntMet),
		maxSize: maxSize,
		lru:     NewLRU(),
		evictQ:  make(chan *EvictTarget),
	}
	accnt.eventQ = make(chan *FlatAccntEvent)
	go accnt.eventLoop()
	return accnt
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
	var met *FlatAccntMet
	var targets []uint32
	var ts uint32
	var size uint64
	var ok bool

	target := a.lru.pop().(EvictTarget)
	if met, ok = a.metrics[target.Metric]; !ok {
		return
	}

	for ts, _ = range met.chunks {
		// if we have chronologically older chunks we add them
		// to the evict targets to avoid fragmentation
		if ts <= target.Ts {
			targets = append(targets, ts)
		}
	}

	sort.Sort(Uint32Asc(targets))

	for _, ts = range targets {
		size = met.chunks[ts]
		met.total = met.total - size
		a.total = a.total - size
		a.evictQ <- &EvictTarget{
			Metric: target.Metric,
			Ts:     ts,
		}
		delete(met.chunks, ts)
	}

	if met.total <= 0 {
		delete(a.metrics, target.Metric)
	}

}

func (a *FlatAccnt) GetEvictQ() chan *EvictTarget {
	return a.evictQ
}
