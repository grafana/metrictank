package cache

import (
	"sort"
	"sync"

	"github.com/raintank/metrictank/mdata/cache/accnt"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/worldping-api/pkg/log"
)

type CCacheMetric struct {
	sync.RWMutex

	// points to the timestamp of the newest cache chunk currently held
	// the head of the linked list that containes the cache chunks
	newest uint32

	// points to the timestamp of the oldest cache chunk currently held
	// the tail of the linked list that containes the cache chunks
	oldest uint32

	// points at cached data chunks, indexed by their according time stamp
	chunks map[uint32]*CCacheChunk
}

func NewCCacheMetric() *CCacheMetric {
	return &CCacheMetric{
		chunks: make(map[uint32]*CCacheChunk),
	}
}

func (mc *CCacheMetric) Init(prev uint32, itergen chunk.IterGen) {
	mc.Add(prev, itergen)
	mc.oldest = itergen.Ts()
	mc.newest = itergen.Ts()
}

func (mc *CCacheMetric) Del(ts uint32) int {
	mc.Lock()
	defer mc.Unlock()

	if _, ok := mc.chunks[ts]; !ok {
		return len(mc.chunks)
	}

	prev := mc.chunks[ts].Prev
	next := mc.chunks[ts].Next

	if _, ok := mc.chunks[prev]; prev != 0 && ok {
		mc.chunks[prev].Next = 0
	}
	if _, ok := mc.chunks[next]; next != 0 && ok {
		mc.chunks[next].Prev = 0
	}

	delete(mc.chunks, ts)

	return len(mc.chunks)
}

func (mc *CCacheMetric) Add(prev uint32, itergen chunk.IterGen) {
	ts := itergen.Ts()

	mc.Lock()
	defer mc.Unlock()

	if _, ok := mc.chunks[ts]; ok {
		// chunk is already present. no need to error on that, just ignore it
		return
	}

	mc.chunks[ts] = &CCacheChunk{
		Ts:    ts,
		Prev:  0,
		Next:  0,
		Itgen: itergen,
	}

	nextTs := mc.nextTs(ts)

	log.Debug("CCacheMetric Add: caching chunk ts %d, nextTs %d", ts, nextTs)

	// if the previous chunk is cached, link in both directions
	if _, ok := mc.chunks[prev]; ok {
		mc.chunks[prev].Next = ts
		mc.chunks[ts].Prev = prev
	}

	// if nextTs() can't figure out the end date it returns ts
	if nextTs > ts {
		// if the next chunk is cached, link in both directions
		if _, ok := mc.chunks[nextTs]; ok {
			mc.chunks[nextTs].Prev = ts
			mc.chunks[ts].Next = nextTs
		}
	}

	// update list head/tail if necessary
	if ts > mc.newest {
		mc.newest = ts
	} else if ts < mc.oldest {
		mc.oldest = ts
	}

	return
}

// get sorted slice of all chunk timestamps
// assumes we have at least read lock
func (mc *CCacheMetric) sortedTs() []uint32 {
	keys := make([]uint32, 0, len(mc.chunks))
	for k := range mc.chunks {
		keys = append(keys, k)
	}
	sort.Sort(accnt.Uint32Asc(keys))
	return keys
}

// nextTs takes a chunk's ts and returns the ts of the next chunk. (guessing if necessary)
// assumes we already have at least a read lock
func (mc *CCacheMetric) nextTs(ts uint32) uint32 {
	chunk := mc.chunks[ts]
	span := chunk.Itgen.Span()
	if span > 0 {
		// if the chunk is span-aware we don't need anything else
		return chunk.Ts + span
	}

	if chunk.Next == 0 {
		if chunk.Prev == 0 {
			// if a chunk has no next and no previous chunk we have to assume it's length is 0
			return chunk.Ts
		} else {
			// if chunk has no next chunk, but has a previous one, we assume the length of this one is same as the previous one
			return chunk.Ts + (chunk.Ts - chunk.Prev)
		}
	} else {
		// if chunk has a next chunk, then that's the ts we need
		return chunk.Next
	}
}

// seekAsc finds the t0 of the chunk that contains ts, by searching from old to recent
// if not found or can't be sure returns 0, false
// assumes we already have at least a read lock
func (mc *CCacheMetric) seekAsc(ts uint32, keys []uint32) (uint32, bool) {
	log.Debug("CCacheMetric seekAsc: seeking for %d in the keys %+d", ts, keys)

	for i := 0; i < len(keys) && keys[i] <= ts; i++ {
		if mc.nextTs(keys[i]) > ts {
			log.Debug("CCacheMetric seekAsc: seek found ts %d is between %d and %d", ts, keys[i], mc.nextTs(keys[i]))
			return keys[i], true
		}
	}

	log.Debug("CCacheMetric seekAsc: seekAsc unsuccessful")
	return 0, false
}

// seekDesc finds the t0 of the chunk that contains ts, by searching from recent to old
// if not found or can't be sure returns 0, false
// assumes we already have at least a read lock
func (mc *CCacheMetric) seekDesc(ts uint32, keys []uint32) (uint32, bool) {
	log.Debug("CCacheMetric seekDesc: seeking for %d in the keys %+d", ts, keys)

	for i := len(keys) - 1; i >= 0 && mc.nextTs(keys[i]) > ts; i-- {
		if keys[i] <= ts {
			log.Debug("CCacheMetric seekDesc: seek found ts %d is between %d and %d", ts, keys[i], mc.nextTs(keys[i]))
			return keys[i], true
		}
	}

	log.Debug("CCacheMetric seekDesc: seekDesc unsuccessful")
	return 0, false
}

func (mc *CCacheMetric) searchForward(from, until uint32, keys []uint32, res *CCSearchResult) {
	ts, ok := mc.seekAsc(from, keys)
	if !ok {
		return
	}

	// add all consecutive chunks to search results, starting at the one containing "from"
	for ; ts != 0; ts = mc.chunks[ts].Next {
		log.Debug("CCacheMetric searchForward: forward search adds chunk ts %d to start", ts)
		res.Start = append(res.Start, mc.chunks[ts].Itgen)
		nextTs := mc.nextTs(ts)
		res.From = nextTs

		if nextTs >= until {
			res.Complete = true
			break
		}
	}
}

func (mc *CCacheMetric) searchBackward(from, until uint32, keys []uint32, res *CCSearchResult) {
	ts, ok := mc.seekDesc(until-1, keys)
	if !ok {
		return
	}

	for ; ts != 0; ts = mc.chunks[ts].Prev {
		log.Debug("CCacheMetric searchBackward: backward search adds chunk ts %d to end", ts)
		res.End = append(res.End, mc.chunks[ts].Itgen)
		startTs := mc.chunks[ts].Ts
		res.Until = startTs

		if startTs <= from {
			break
		}
	}
}

// the idea of this method is that we first look for the chunks where the
// "from" and "until" ts are in. then we seek from the "from" towards "until"
// and add as many cunks as possible to the result, if this did not result
// in all chunks necessary to serve the request we do the same in the reverse
// order from "until" to "from"
// if the first seek in chronological direction already ends up with all the
// chunks we need to serve the request, the second one can be skipped.

// EXAMPLE:
// from ts:                    |
// until ts:                                                   |
// cache:            |---|---|---|   |   |   |   |   |---|---|---|---|---|---|
// chunks returned:          |---|                   |---|---|---|
//
func (mc *CCacheMetric) Search(res *CCSearchResult, from, until uint32) {
	mc.RLock()
	defer mc.RUnlock()

	if len(mc.chunks) < 1 {
		return
	}

	keys := mc.sortedTs()

	mc.searchForward(from, until, keys, res)
	if !res.Complete {
		mc.searchBackward(from, until, keys, res)
	}

	if !res.Complete && res.From > res.Until {
		log.Debug("CCacheMetric Search: Found from > until (%d/%d), printing chunks\n", res.From, res.Until)
		mc.debugMetric()
	}
}

func (mc *CCacheMetric) debugMetric() {
	keys := mc.sortedTs()
	log.Debug("CCacheMetric debugMetric: --- debugging metric ---\n")
	log.Debug("CCacheMetric debugMetric: oldest %d; newest %d\n", mc.oldest, mc.newest)
	for _, key := range keys {
		log.Debug("CCacheMetric debugMetric: ts %d; prev %d; next %d\n", key, mc.chunks[key].Prev, mc.chunks[key].Next)
	}
	log.Debug("CCacheMetric debugMetric: ------------------------\n")
}
