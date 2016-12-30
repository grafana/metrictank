package accnt

import (
	"github.com/raintank/worldping-api/pkg/log"
	"sort"
	"time"
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

	stats       Stats
	statsTicker time.Ticker
	lastPrint   int64
}

type FlatAccntMet struct {
	total  uint64
	chunks map[uint32]uint64
}

// event types to be used in FlatAccntEvent
const evnt_complt_met uint8 = 1
const evnt_miss_met uint8 = 2
const evnt_part_met uint8 = 3
const evnt_hit_chnk uint8 = 4
const evnt_add_chnk uint8 = 5

type FlatAccntEvent struct {
	t  uint8       // event type
	pl interface{} // payload
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
		metrics:     make(map[string]*FlatAccntMet),
		maxSize:     maxSize,
		lru:         NewLRU(),
		evictQ:      make(chan *EvictTarget, evictQSize),
		eventQ:      make(chan FlatAccntEvent, eventQSize),
		lastPrint:   time.Now().UnixNano(),
		statsTicker: *(time.NewTicker(time.Second * 10)),
	}

	go accnt.eventLoop()
	return &accnt
}

func (a *FlatAccnt) AddChunk(metric string, ts uint32, size uint64) {
	a.act(evnt_add_chnk, &AddPayload{metric, ts, size})
}

func (a *FlatAccnt) HitChunk(metric string, ts uint32) {
	a.act(evnt_hit_chnk, &HitPayload{metric, ts})
}

func (a *FlatAccnt) MissMetric() {
	a.act(evnt_miss_met, nil)
}

func (a *FlatAccnt) PartialMetric() {
	a.act(evnt_part_met, nil)
}

func (a *FlatAccnt) CompleteMetric() {
	a.act(evnt_complt_met, nil)
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

func (a *FlatAccnt) statPrintReset() {
	now := time.Now().UnixNano()
	duration := now - a.lastPrint
	var usg uint64 = 0
	a.lastPrint = now

	if a.total != 0 {
		usg = 100 * a.total / a.maxSize
	}

	log.Info("--- Chunk Cache stats for the past %dns ---", duration)
	log.Info("current size/max size %d/%d (%d%%)", a.total, a.maxSize, usg)
	log.Info("metric complete %d", a.stats.complt_met)
	log.Info("metric misses   %d", a.stats.miss_met)
	log.Info("metric partials %d", a.stats.part_met)
	log.Info("metric adds     %d", a.stats.add_met)
	log.Info("metric evicts   %d", a.stats.evict_met)
	log.Info("chunk hits      %d", a.stats.hit_chnk)
	log.Info("chunk adds      %d", a.stats.add_chnk)
	log.Info("chunk evicts    %d", a.stats.evict_chnk)

	a.stats.complt_met = 0
	a.stats.miss_met = 0
	a.stats.part_met = 0
	a.stats.add_met = 0
	a.stats.evict_met = 0
	a.stats.hit_chnk = 0
	a.stats.add_chnk = 0
	a.stats.evict_chnk = 0
}

func (a *FlatAccnt) eventLoop() {
	for {
		select {
		case <-a.statsTicker.C:
			a.statPrintReset()
		case event := <-a.eventQ:
			switch event.t {
			case evnt_add_chnk:
				payload := event.pl.(*AddPayload)
				a.add(payload.metric, payload.ts, payload.size)
				a.stats.add_chnk++
				a.lru.touch(
					EvictTarget{
						Metric: payload.metric,
						Ts:     payload.ts,
					},
				)
			case evnt_hit_chnk:
				payload := event.pl.(*HitPayload)
				a.stats.hit_chnk++
				a.lru.touch(
					EvictTarget{
						Metric: payload.metric,
						Ts:     payload.ts,
					},
				)
			case evnt_miss_met:
				a.stats.miss_met++
			case evnt_part_met:
				a.stats.part_met++
			case evnt_complt_met:
				a.stats.complt_met++
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
		a.stats.add_met++
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
		a.stats.evict_chnk++
		a.evictQ <- &EvictTarget{
			Metric: target.Metric,
			Ts:     ts,
		}
		delete(met.chunks, ts)
	}

	if met.total <= 0 {
		a.stats.evict_met++
		delete(a.metrics, target.Metric)
	}

}

func (a *FlatAccnt) GetEvictQ() chan *EvictTarget {
	return a.evictQ
}
