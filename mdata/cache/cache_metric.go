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

	if prev != 0 {
		mc.chunks[prev].Next = mc.chunks[ts].Next
	}
	if next != 0 {
		mc.chunks[next].Prev = mc.chunks[ts].Prev
	}

	return len(mc.chunks)
}

func (mc *CCacheMetric) Add(prev uint32, itergen chunk.IterGen) {
	var ts, endTs uint32
	ts = itergen.Ts()

	mc.Lock()
	defer mc.Unlock()
	if _, ok := mc.chunks[ts]; ok {
		// chunk is already present. no need to error on that, just ignore it
		return
	}

	mc.chunks[ts] = &CCacheChunk{
		Ts:    ts,
		Prev:  prev,
		Next:  0,
		Itgen: itergen,
	}

	// if the previous chunk is cached, set this one as its next
	if _, ok := mc.chunks[prev]; ok {
		mc.chunks[prev].Next = ts
	}

	endTs = mc.endTs(ts)
	log.Debug("cache: caching chunk ts %d, endTs %d", ts, endTs)

	// if endTs() can't figure out the end date it returns ts
	if endTs != ts {
		if _, ok := mc.chunks[endTs]; ok {
			mc.chunks[endTs].Prev = ts
			mc.chunks[ts].Next = endTs
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

// takes a chunk's ts and returns the end ts (guessing if necessary)
// assumes we already have at least a read lock
func (mc *CCacheMetric) endTs(ts uint32) uint32 {
	span := (*mc.chunks[ts]).Itgen.Span()
	if span > 0 {
		// if the chunk is span-aware we don't need anything else
		return (*mc.chunks[ts]).Ts + span
	}

	if (*mc.chunks[ts]).Next == 0 {
		if (*mc.chunks[ts]).Prev == 0 {
			// if a chunk has no next and no previous chunk we have to assume it's length is 0
			return (*mc.chunks[ts]).Ts
		} else {
			// if chunk has no next chunk, but has a previous one, we assume the length of this one is same as the previous one
			return (*mc.chunks[ts]).Ts + ((*mc.chunks[ts]).Ts - (*mc.chunks[(*mc.chunks[ts]).Prev]).Ts)
		}
	} else {
		// if chunk has a next chunk, then the end ts of this chunk is the start ts of the next one
		return (*mc.chunks[(*mc.chunks[ts]).Next]).Ts
	}
}

// assumes we already have at least a read lock
// asc determines the direction of the search, ascending or descending
func (mc *CCacheMetric) seek(ts uint32, keys []uint32, asc bool) (uint32, bool) {
	var seekpos int
	var shiftby int

	log.Debug("cache: seeking for %d in the keys %+d", ts, keys)
	if asc {
		// if ascending start searching at the first
		seekpos = 0
		shiftby = 1
	} else {
		// if descending start searching at the last
		seekpos = len(keys) - 1
		shiftby = -1
	}

	for {
		if asc {
			if seekpos >= len(keys) || keys[seekpos] > ts {
				break
			}
		} else {
			if seekpos < 0 || mc.endTs(keys[seekpos]) < ts {
				break
			}
		}

		if keys[seekpos] <= ts && mc.endTs(keys[seekpos]) > ts {
			log.Debug("cache: seek found ts %d is between %d and %d", ts, keys[seekpos], mc.endTs(keys[seekpos]))
			return keys[seekpos], true
		}

		seekpos = seekpos + shiftby
	}

	log.Debug("cache: seek unsuccessful")
	return 0, false
}

func (mc *CCacheMetric) searchForward(from uint32, until uint32, keys []uint32, res *CCSearchResult) {
	ts, ok := mc.seek(from, keys, true)
	if !ok {
		return
	}

	// add all consecutive chunks to search results, starting at the one containing "from"
	for ; ts != 0; ts = mc.chunks[ts].Next {
		log.Debug("cache: forward search adds chunk ts %d to start", ts)
		res.Start = append(res.Start, mc.chunks[ts].Itgen)
		endts := mc.endTs(ts)
		res.From = endts

		if endts >= until {
			res.Complete = true
			break
		}
	}
}

func (mc *CCacheMetric) searchBackward(from uint32, until uint32, keys []uint32, res *CCSearchResult) {
	ts, ok := mc.seek(until, keys, false)
	if !ok {
		return
	}

	for ; ts != 0; ts = mc.chunks[ts].Prev {
		log.Debug("cache: backward search adds chunk ts %d to start", ts)
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
func (mc *CCacheMetric) Search(res *CCSearchResult, from uint32, until uint32) {
	mc.RLock()
	defer mc.RUnlock()

	if len(mc.chunks) < 1 {
		return
	}

	keys := mc.sortedTs()

	mc.searchForward(from, until-1, keys, res)
	if !res.Complete {
		mc.searchBackward(from, until-1, keys, res)
	}
}
