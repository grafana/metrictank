package accnt

import (
	"github.com/raintank/worldping-api/pkg/log"
	"sort"
)

const evictQSize = 1000

// it's easily possible for many events to happen in one request,
// we never want this to fill up because otherwise events get dropped
const eventQSize = 100000

// FlatAccnt implements Flat accounting.
// Keeps track of the chunk cache size and in which order the contained
// chunks have been used to last time. If it detects that the total cache
// size is above the given limit, it feeds the least recently used
// cache chunks into the evict queue, which will get consumed by the
// evict loop.
type FlatAccnt struct {
	// total size of cache we're accounting for
	total uint64

	// metric accounting per metric key
	metrics map[string]*FlatAccntMet

	// the size limit, once this is reached we'll start evicting data
	maxSize uint64

	// a last-recently-used implementation that keeps track of all chunks
	// and which hasn't been used for the longest time. the eviction
	// function relies on this to know what to evict.
	lru *LRU

	// whenever a chunk gets evicted a job gets added to this queue. it is
	// consumed by the chunk cache, which will evict whatever the jobs in
	// evictQ tell it to
	evictQ chan *EvictTarget

	// a queue of add and hit events to be processed by the accounting.
	// each add means data got added to the cache, each hit means data
	// has been accessed and hence the LRU needs to be updated.
	eventQ chan FlatAccntEvent
}

type FlatAccntMet struct {
	total  uint64
	chunks map[uint32]uint64
}

// event types to be used in FlatAccntEvent
const evnt_hit_chnk uint8 = 4
const evnt_add_chnk uint8 = 5
const evnt_stop uint8 = 100
const evnt_reset uint8 = 101

type FlatAccntEvent struct {
	t  uint8       // event type
	pl interface{} // payload
}

// payload to be sent with an add event
type AddPayload struct {
	metric string
	ts     uint32
	size   uint64
}

// payload to be sent with a hit event
type HitPayload struct {
	metric string
	ts     uint32
}

func NewFlatAccnt(maxSize uint64) *FlatAccnt {
	accnt := FlatAccnt{
		metrics: make(map[string]*FlatAccntMet),
		maxSize: maxSize,
		lru:     NewLRU(),
		evictQ:  make(chan *EvictTarget, evictQSize),
		eventQ:  make(chan FlatAccntEvent, eventQSize),
	}
	cacheSizeMax.SetUint64(maxSize)

	go accnt.eventLoop()
	return &accnt
}

func (a *FlatAccnt) AddChunk(metric string, ts uint32, size uint64) {
	a.act(evnt_add_chnk, &AddPayload{metric, ts, size})
}

func (a *FlatAccnt) HitChunk(metric string, ts uint32) {
	a.act(evnt_hit_chnk, &HitPayload{metric, ts})
}

func (a *FlatAccnt) Stop() {
	a.act(evnt_stop, nil)
}

func (a *FlatAccnt) Reset() {
	a.act(evnt_reset, nil)
}

func (a *FlatAccnt) act(t uint8, payload interface{}) {
	event := FlatAccntEvent{
		t:  t,
		pl: payload,
	}

	select {
	// we never want to block for accounting, rather just let it miss some events and print an error
	case a.eventQ <- event:
	default:
		log.Error(3, "Failed to submit event to accounting, channel was blocked")
	}
}

func (a *FlatAccnt) eventLoop() {
	for {
		select {
		case event := <-a.eventQ:
			switch event.t {
			case evnt_add_chnk:
				payload := event.pl.(*AddPayload)
				a.add(payload.metric, payload.ts, payload.size)
				cacheChunkAdd.Inc()
				a.lru.touch(
					EvictTarget{
						Metric: payload.metric,
						Ts:     payload.ts,
					},
				)
			case evnt_hit_chnk:
				payload := event.pl.(*HitPayload)
				a.lru.touch(
					EvictTarget{
						Metric: payload.metric,
						Ts:     payload.ts,
					},
				)
			case evnt_stop:
				return
			case evnt_reset:
				a.metrics = make(map[string]*FlatAccntMet)
				a.total = 0
				a.lru.reset()
			}

			// evict until we're below the max
			for a.total > a.maxSize {
				a.evict()
			}
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
		cacheMetricAdd.Inc()
	}

	if _, ok = met.chunks[ts]; ok {
		// we already have that chunk
		return
	}

	met.chunks[ts] = size
	met.total = met.total + size
	a.total = a.total + size
	cacheSizeUsed.SetUint64(a.total)
}

func (a *FlatAccnt) evict() {
	var met *FlatAccntMet
	var targets []uint32
	var ts uint32
	var size uint64
	var ok bool
	var e interface{}
	var target EvictTarget

	e = a.lru.pop()

	// got nothing to evict
	if e == nil {
		return
	}

	// convert to EvictTarget otherwise
	target = e.(EvictTarget)

	if met, ok = a.metrics[target.Metric]; !ok {
		return
	}

	for ts = range met.chunks {
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
		cacheSizeUsed.SetUint64(a.total)
		cacheChunkEvict.Inc()
		a.evictQ <- &EvictTarget{
			Metric: target.Metric,
			Ts:     ts,
		}
		delete(met.chunks, ts)
	}

	if met.total <= 0 {
		cacheMetricEvict.Inc()
		delete(a.metrics, target.Metric)
	}

}

func (a *FlatAccnt) GetEvictQ() chan *EvictTarget {
	return a.evictQ
}
