package accnt

import (
	"sort"
	"time"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/raintank/schema"
)

const (
	evictQSize = 1000

	// these numbers are estimates and could still use some fine tuning
	// they do not account for map growth

	// EvictTarget (24 bytes + 4 bytes) (AMKey, uint32) + 8 bytes (pointer from map[interface{}]*EvictTarget)
	// + 40 bytes (Element in List)
	lruItemSize = 76

	// k: 4 bytes + v: 8 bytes (map[uint32]uint64)
	famChunkSize = 12
	// k: 24 bytes + v: 16 bytes (map[schema.AMKey]*FlatAccntMet)
	famSize = 40

	// 24 bytes + 8 bytes + 24 bytes + 20 bytes (sync.RWMutex, map, slice, MKey) + 4 bytes for alignment
	// + 24 bytes for map entry in CCache schema.AMKey (map[schema.AMKey]*CCacheMetric)
	ccmSize = 104
	// k: 4 bytes + v: 48 bytes (map[uint32]chunk.IterGen) + 4 bytes for []keys
	ccmChunkSize = 56
)

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

type FlatAccntMet struct {
	total  uint64
	chunks map[uint32]uint64
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
	size   uint64
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
	accntEventQueueMax.SetUint64(uint64(EventQSize))

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

func (a *FlatAccnt) AddChunk(metric schema.AMKey, ts uint32, size uint64) {
	a.act(evnt_add_chnk, &AddPayload{metric, ts, size})
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

	pre := time.Now()
	a.eventQ <- event
	accntEventAddDuration.Value(time.Now().Sub(pre))
	accntEventQueueUsed.Value(len(a.eventQ))
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
				cacheOverheadChunk.SetUint64(0)
				cacheOverheadFlat.SetUint64(0)
				cacheOverheadLru.SetUint64(0)
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

	lenChunks := len(met.chunks)
	cacheSizeUsed.DecUint64(met.total)
	cacheOverheadFlat.DecUint64(uint64(lenChunks*famChunkSize + famSize))
	cacheOverheadLru.DecUint64(uint64(lenChunks * lruItemSize))
	cacheOverheadChunk.DecUint64(uint64(lenChunks*ccmChunkSize + ccmSize))

	for ts := range met.chunks {
		a.lru.del(
			EvictTarget{
				Metric: metric,
				Ts:     ts,
			},
		)
	}

	delete(a.metrics, metric)
}

func (a *FlatAccnt) add(metric schema.AMKey, ts uint32, size uint64) {
	var met *FlatAccntMet
	var ok bool
	var totalFlat, totalChunk, totalLru uint64

	if met, ok = a.metrics[metric]; !ok {
		met = &FlatAccntMet{
			total:  0,
			chunks: make(map[uint32]uint64),
		}
		a.metrics[metric] = met
		cacheMetricAdd.Inc()
		totalFlat += famSize
		totalChunk += ccmSize
	}

	if _, ok = met.chunks[ts]; ok {
		// we already have that chunk
		return
	}

	met.chunks[ts] = size

	totalFlat += famChunkSize
	totalChunk += ccmChunkSize
	// this func is called from the event loop so lru will be touched with new EvictTarget
	totalLru += lruItemSize
	met.total = met.total + size
	cacheSizeUsed.AddUint64(size)
	cacheOverheadFlat.AddUint64(totalFlat)
	cacheOverheadChunk.AddUint64(totalChunk)
	cacheOverheadLru.AddUint64(totalLru)
}

func (a *FlatAccnt) addRange(metric schema.AMKey, chunks []chunk.IterGen) {
	var met *FlatAccntMet
	var ok bool
	var totalFlat, totalChunk, totalLru uint64

	if met, ok = a.metrics[metric]; !ok {
		met = &FlatAccntMet{
			total:  0,
			chunks: make(map[uint32]uint64),
		}
		a.metrics[metric] = met
		cacheMetricAdd.Inc()
		totalFlat += famSize
		totalChunk += ccmSize
	}

	var sizeDiff uint64

	for _, chunk := range chunks {
		if _, ok = met.chunks[chunk.Ts]; ok {
			// we already have that chunk
			continue
		}
		size := chunk.Size()
		sizeDiff += size
		met.chunks[chunk.Ts] = size
		totalFlat += famChunkSize
		totalChunk += ccmChunkSize
		// this func is called from the event loop so lru will be touched with new EvictTarget
		totalLru += lruItemSize
	}

	met.total = met.total + sizeDiff
	cacheSizeUsed.AddUint64(sizeDiff)
	cacheOverheadFlat.AddUint64(totalFlat)
	cacheOverheadChunk.AddUint64(totalChunk)
	cacheOverheadLru.AddUint64(totalLru)
}

func (a *FlatAccnt) evict() {
	var met *FlatAccntMet
	var targets []uint32
	var ts uint32
	var size uint64
	var ok bool
	var e interface{}
	var target EvictTarget
	var totalFlat, totalChunk uint64

	e = a.lru.pop()

	// got nothing to evict
	if e == nil {
		return
	}

	// convert to EvictTarget otherwise
	target = e.(EvictTarget)
	// the item is already removed from the LRU and will not be re-added in this call path
	// so it is safe to decrement the stat
	cacheOverheadLru.DecUint64(lruItemSize)

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

	lenChunks := len(targets)
	for _, ts = range targets {
		size = met.chunks[ts]
		met.total = met.total - size
		cacheSizeUsed.DecUint64(size)
		cacheChunkEvict.Inc()
		a.evictQ <- &EvictTarget{
			Metric: target.Metric,
			Ts:     ts,
		}
		delete(met.chunks, ts)
	}

	// technically none of the *CCacheChunk or *CCacheMetric will be deleted until
	// after CCache processes its evictLoop, so these reductions in size
	// might be inaccurate for a short period of time. In an environment where more
	// items are waiting to be added to the cache this could allow the cache to grow more
	// than it should until the evictions can be processed in ccache and then the
	// memory reclaimed by GC at some time in the hopefully near future

	totalChunk += uint64(lenChunks * ccmChunkSize)
	totalFlat += uint64(lenChunks * famChunkSize)

	if met.total <= 0 {
		cacheMetricEvict.Inc()
		delete(a.metrics, target.Metric)
		totalChunk += ccmSize
		totalFlat += famSize
	}

	cacheOverheadChunk.DecUint64(totalChunk)
	cacheOverheadFlat.DecUint64(totalFlat)

}

func (a *FlatAccnt) GetEvictQ() chan *EvictTarget {
	return a.evictQ
}
