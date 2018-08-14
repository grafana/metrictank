package accnt

import (
	"sort"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

const evictQSize = 1000

// it's easily possible for many events to happen in one request,
// we never want this to fill up because otherwise events get dropped
var EventQSize = 100000

// FlatAccnt implements Flat accounting.
// Keeps track of the chunk cache size and in which order the contained
// chunks have been used to last time. If it detects that the total cache
// size is above the given limit, it feeds the least recently used
// cache chunks into the evict queue, which will get consumed by the
// evict loop.
type FlatAccnt struct {
	// metric accounting per metric key
	metrics map[schema.AMKey]*FlatAccntMet

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

type chunkSize struct {
	len uint64
	cap uint64
}

type FlatAccntMet struct {
	totalLen uint64
	totalCap uint64
	chunks   map[uint32]chunkSize
}

type FlatAccntEvent struct {
	eType eventType
	pl    interface{} // payload
}

type eventType uint8

const (
	evnt_hit_chnk eventType = iota
	evnt_hit_chnks
	evnt_add_chnk
	evnt_add_chnks
	evnt_del_met
	evnt_get_total
	evnt_stop
	evnt_reset
)

// payload to be sent with an add event
type AddPayload struct {
	metric schema.AMKey
	ts     uint32
	size   chunkSize
}

// payload to be sent with an add event
type AddsPayload struct {
	metric schema.AMKey
	chunks []chunk.IterGen
}

// payload to be sent with a hit event
type HitPayload struct {
	metric schema.AMKey
	ts     uint32
}

// payload to be sent with a hits event
type HitsPayload struct {
	metric schema.AMKey
	chunks []chunk.IterGen
}

// payload to be sent with del metric event
type DelMetPayload struct {
	metric schema.AMKey
}

// payload to be sent with a get total request event
type GetTotalPayload struct {
	res_chan chan uint64
}

func NewFlatAccnt(maxSize uint64) *FlatAccnt {
	accnt := FlatAccnt{
		metrics: make(map[schema.AMKey]*FlatAccntMet),
		maxSize: maxSize,
		lru:     NewLRU(),
		evictQ:  make(chan *EvictTarget, evictQSize),
		eventQ:  make(chan FlatAccntEvent, EventQSize),
	}
	cacheSizeMax.SetUint64(maxSize)

	go accnt.eventLoop()
	return &accnt
}

func (a *FlatAccnt) DelMetric(metric schema.AMKey) {
	a.act(evnt_del_met, &DelMetPayload{metric})
}

func (a *FlatAccnt) GetTotal() uint64 {
	res_chan := make(chan uint64)
	a.act(evnt_get_total, &GetTotalPayload{res_chan})
	return <-res_chan
}

func (a *FlatAccnt) AddChunk(metric schema.AMKey, ts uint32, len, cap uint64) {
	a.act(evnt_add_chnk, &AddPayload{metric, ts, chunkSize{len: len, cap: cap}})
}

func (a *FlatAccnt) AddChunks(metric schema.AMKey, chunks []chunk.IterGen) {
	a.act(evnt_add_chnks, &AddsPayload{metric, chunks})
}

func (a *FlatAccnt) HitChunk(metric schema.AMKey, ts uint32) {
	a.act(evnt_hit_chnk, &HitPayload{metric, ts})
}
func (a *FlatAccnt) HitChunks(metric schema.AMKey, chunks []chunk.IterGen) {
	if len(chunks) == 0 {
		return
	}
	a.act(evnt_hit_chnks, &HitsPayload{metric, chunks})
}

func (a *FlatAccnt) Stop() {
	a.act(evnt_stop, nil)
}

func (a *FlatAccnt) Reset() {
	a.act(evnt_reset, nil)
}

func (a *FlatAccnt) act(eType eventType, payload interface{}) {
	event := FlatAccntEvent{
		eType: eType,
		pl:    payload,
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
			switch event.eType {
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
			case evnt_add_chnks:
				payload := event.pl.(*AddsPayload)
				a.addRange(payload.metric, payload.chunks)
				cacheChunkAdd.Add(len(payload.chunks))
				for _, chunk := range payload.chunks {
					a.lru.touch(
						EvictTarget{
							Metric: payload.metric,
							Ts:     chunk.Ts,
						},
					)
				}
			case evnt_hit_chnk:
				payload := event.pl.(*HitPayload)
				a.lru.touch(
					EvictTarget{
						Metric: payload.metric,
						Ts:     payload.ts,
					},
				)
			case evnt_hit_chnks:
				payload := event.pl.(*HitsPayload)
				for _, chunk := range payload.chunks {
					a.lru.touch(
						EvictTarget{
							Metric: payload.metric,
							Ts:     chunk.Ts,
						},
					)
				}
			case evnt_del_met:
				payload := event.pl.(*DelMetPayload)
				a.delMet(payload.metric)
			case evnt_get_total:
				payload := event.pl.(*GetTotalPayload)
				a.getTotal(payload.res_chan)
			case evnt_stop:
				return
			case evnt_reset:
				a.metrics = make(map[schema.AMKey]*FlatAccntMet)
				a.lru.reset()
				cacheSizeUsed.SetUint64(0)
				cacheCapUsed.SetUint64(0)
			}

			// evict until we're below the max
			for cacheSizeUsed.Peek() > a.maxSize {
				a.evict()
			}
		}
	}
}

func (a *FlatAccnt) getTotal(res_chan chan uint64) {
	res_chan <- cacheSizeUsed.Peek()
}

func (a *FlatAccnt) delMet(metric schema.AMKey) {
	met, ok := a.metrics[metric]
	if !ok {
		return
	}

	for ts := range met.chunks {
		a.lru.del(
			EvictTarget{
				Metric: metric,
				Ts:     ts,
			},
		)
	}

	cacheSizeUsed.DecUint64(met.totalLen)
	cacheCapUsed.DecUint64(met.totalCap)
	delete(a.metrics, metric)
}

func (a *FlatAccnt) add(metric schema.AMKey, ts uint32, size chunkSize) {
	var met *FlatAccntMet
	var ok bool

	if met, ok = a.metrics[metric]; !ok {
		met = &FlatAccntMet{
			totalLen: 0,
			totalCap: 0,
			chunks:   make(map[uint32]chunkSize),
		}
		a.metrics[metric] = met
		cacheMetricAdd.Inc()
	} else {
		if _, ok = met.chunks[ts]; ok {
			// we already have that chunk
			return
		}
	}

	met.chunks[ts] = size
	met.totalLen = met.totalLen + size.len
	met.totalCap = met.totalCap + size.cap
	cacheSizeUsed.AddUint64(size.len)
	cacheCapUsed.AddUint64(size.cap)
}

func (a *FlatAccnt) addRange(metric schema.AMKey, chunks []chunk.IterGen) {
	var met *FlatAccntMet
	var ok bool

	if met, ok = a.metrics[metric]; !ok {
		met = &FlatAccntMet{
			totalLen: 0,
			totalCap: 0,
			chunks:   make(map[uint32]chunkSize),
		}
		a.metrics[metric] = met
		cacheMetricAdd.Inc()
	}

	var lenDiff uint64
	var capDiff uint64

	for _, chunk := range chunks {
		if _, ok = met.chunks[chunk.Ts]; ok {
			// we already have that chunk
			continue
		}
		size := chunkSize{len: uint64(len(chunk.B)), cap: uint64(cap(chunk.B))}
		lenDiff += size.len
		capDiff += size.cap
		met.chunks[chunk.Ts] = size
	}

	met.totalLen = met.totalLen + lenDiff
	met.totalCap = met.totalCap + capDiff
	cacheSizeUsed.AddUint64(lenDiff)
	cacheCapUsed.AddUint64(capDiff)
}

func (a *FlatAccnt) evict() {
	var met *FlatAccntMet
	var targets []uint32
	var ts uint32
	var size chunkSize
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
		met.totalLen = met.totalLen - size.len
		met.totalCap = met.totalCap - size.cap
		cacheSizeUsed.DecUint64(size.len)
		cacheCapUsed.DecUint64(size.cap)
		cacheChunkEvict.Inc()
		a.evictQ <- &EvictTarget{
			Metric: target.Metric,
			Ts:     ts,
		}
		delete(met.chunks, ts)
	}

	if met.totalLen <= 0 {
		cacheMetricEvict.Inc()
		delete(a.metrics, target.Metric)
	}

}

func (a *FlatAccnt) GetEvictQ() chan *EvictTarget {
	return a.evictQ
}
