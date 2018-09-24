package cache

import (
	"context"
	"sort"
	"sync"

	"github.com/grafana/metrictank/mdata/cache/accnt"
	"github.com/grafana/metrictank/mdata/chunk"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

// CCacheMetric caches data chunks
type CCacheMetric struct {
	sync.RWMutex

	// cached data chunks by timestamp
	chunks map[uint32]*CCacheChunk

	// chunk time stamps in ascending order
	keys []uint32

	MKey schema.MKey
}

// NewCCacheMetric creates a CCacheMetric
func NewCCacheMetric(mkey schema.MKey) *CCacheMetric {
	return &CCacheMetric{
		MKey:   mkey,
		chunks: make(map[uint32]*CCacheChunk),
	}
}

// Del deletes chunks for the given timestamp
func (mc *CCacheMetric) Del(ts uint32) int {
	mc.Lock()
	defer mc.Unlock()

	if _, ok := mc.chunks[ts]; !ok {
		return len(mc.chunks)
	}

	prev := mc.chunks[ts].Prev
	next := mc.chunks[ts].Next

	if prev != 0 {
		if _, ok := mc.chunks[prev]; ok {
			mc.chunks[prev].Next = 0
		}
	}

	if next != 0 {
		if _, ok := mc.chunks[next]; ok {
			mc.chunks[next].Prev = 0
		}
	}

	delete(mc.chunks, ts)

	// regenerate the list of sorted keys after deleting a chunk
	// NOTE: we can improve perf by just taking out the ts (partially rewriting
	// the slice in one go), can we also batch deletes?
	mc.generateKeys()

	return len(mc.chunks)
}

// AddRange adds a range (sequence) of chunks.
// Note the following requirements:
// the sequence should be in ascending timestamp order
// the sequence should be complete (no gaps)
func (mc *CCacheMetric) AddRange(prev uint32, itergens []chunk.IterGen) {
	if len(itergens) == 0 {
		return
	}

	if len(itergens) == 1 {
		mc.Add(prev, itergens[0])
		return
	}

	mc.Lock()
	defer mc.Unlock()

	// pre-allocate 1 slice, cheaper than allocating one by one
	chunks := make([]CCacheChunk, 0, len(itergens))

	// handle the first one
	itergen := itergens[0]
	ts := itergen.Ts

	// if we add data that is older than chunks already cached,
	// we will have to sort the keys once we're done adding them
	sortKeys := len(mc.keys) > 0 && mc.keys[len(mc.keys)-1] > ts

	// add chunk if we don't have it yet (most likely)
	if _, ok := mc.chunks[ts]; !ok {

		// if previous chunk has not been passed we try to be smart and figure it out.
		// this is common in a scenario where a metric continuously gets queried
		// for a range that starts less than one chunkspan before now().
		res, ok := mc.seekDesc(ts - 1)
		if ok {
			if prev == 0 {
				prev = res
			} else if prev != res {
				log.WithFields(log.Fields{
					"key":  mc.MKey.String(),
					"prev": prev,
					"seek": res,
				}).Warn("CCacheMetric: AddRange(), 'prev' param disagrees with seek")
			}
		}

		// if the previous chunk is cached, link it
		if prev != 0 && (ts-prev) != (itergens[1].Ts-ts) {
			log.WithFields(log.Fields{
				"key":           mc.MKey.String(),
				"prev":          prev,
				"itergens.0.ts": itergens[0].Ts,
				"itergens.1.ts": itergens[1].Ts,
			}).Warn("CCacheMetric: AddRange(), bad prev being used")
			prev = 0
		} else if _, ok := mc.chunks[prev]; ok {
			mc.chunks[prev].Next = ts
		} else {
			prev = 0
		}

		chunks = append(chunks, CCacheChunk{
			Ts:    ts,
			Prev:  prev,
			Next:  itergens[1].Ts,
			Itgen: itergen,
		})
		mc.chunks[ts] = &chunks[len(chunks)-1]
		mc.keys = append(mc.keys, ts)
	} else {
		mc.chunks[ts].Next = itergens[1].Ts
	}

	prev = ts

	// handle the 2nd until the last-but-one
	for i := 1; i < len(itergens)-1; i++ {
		itergen = itergens[i]
		ts = itergen.Ts
		// Only append key if we didn't have this chunk already
		if _, ok := mc.chunks[ts]; !ok {
			mc.keys = append(mc.keys, ts)
		}

		// add chunk, potentially overwriting pre-existing chunk (unlikely)
		chunks = append(chunks, CCacheChunk{
			Ts:    ts,
			Prev:  prev,
			Next:  itergens[i+1].Ts,
			Itgen: itergen,
		})
		mc.chunks[ts] = &chunks[len(chunks)-1]

		prev = ts
	}

	// handle the last one
	itergen = itergens[len(itergens)-1]
	ts = itergen.Ts

	// add chunk if we don't have it yet (most likely)
	if _, ok := mc.chunks[ts]; !ok {

		// if nextTs() can't figure out the end date it returns ts
		next := mc.nextTsCore(itergen, prev, 0)
		if next == ts {
			next = 0
		} else {
			// if the next chunk is cached, link in both directions
			if _, ok := mc.chunks[next]; ok {
				mc.chunks[next].Prev = ts
			} else {
				next = 0
			}
		}

		chunks = append(chunks, CCacheChunk{
			Ts:    ts,
			Prev:  prev,
			Next:  next,
			Itgen: itergen,
		})
		mc.chunks[ts] = &chunks[len(chunks)-1]
		mc.keys = append(mc.keys, ts)
	} else {
		mc.chunks[ts].Prev = prev
	}

	if sortKeys {
		sort.Sort(accnt.Uint32Asc(mc.keys))
	}

	return
}

// Add adds a chunk to the cache
func (mc *CCacheMetric) Add(prev uint32, itergen chunk.IterGen) {
	ts := itergen.Ts

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

	log.WithFields(log.Fields{
		"timestamp":      ts,
		"next.timestamp": nextTs,
	}).Debug("CCacheMetric: Add(), caching chunk")

	// if previous chunk has not been passed we try to be smart and figure it out.
	// this is common in a scenario where a metric continuously gets queried
	// for a range that starts less than one chunkspan before now().
	res, ok := mc.seekDesc(ts - 1)
	if ok {
		if prev == 0 {
			prev = res
		} else if prev != res {
			log.WithFields(log.Fields{
				"key":  mc.MKey.String(),
				"prev": prev,
				"seek": res,
			}).Warn("CCacheMetric: Add(), 'prev' param disagrees with seek")
		}
	}

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

	// assure key is added to mc.keys

	// if no keys yet, just add it and it's sorted
	if len(mc.keys) == 0 {
		mc.keys = append(mc.keys, ts)
		return
	}

	// add the ts, and sort if necessary
	mc.keys = append(mc.keys, ts)
	if mc.keys[len(mc.keys)-1] < mc.keys[len(mc.keys)-2] {
		sort.Sort(accnt.Uint32Asc(mc.keys))
	}
}

// generateKeys generates sorted slice of all chunk timestamps
// assumes we have at least read lock
func (mc *CCacheMetric) generateKeys() {
	keys := make([]uint32, 0, len(mc.chunks))
	for k := range mc.chunks {
		keys = append(keys, k)
	}
	sort.Sort(accnt.Uint32Asc(keys))
	mc.keys = keys
}

// nextTs takes a chunk's ts and returns the ts of the next chunk. (guessing if necessary)
// assumes we already have at least a read lock
func (mc *CCacheMetric) nextTs(ts uint32) uint32 {
	chunk := mc.chunks[ts]
	return mc.nextTsCore(chunk.Itgen, chunk.Prev, chunk.Next)
}

// nextTsCore returns the ts of the next chunk, given a chunks key properties
// (to the extent we know them). It guesses if necessary.
// assumes we already have at least a read lock
func (mc *CCacheMetric) nextTsCore(itgen chunk.IterGen, prev, next uint32) uint32 {
	span := itgen.Span
	if span > 0 {
		// if the chunk is span-aware we don't need anything else
		return itgen.Ts + span
	}

	// if chunk has a next chunk, then that's the ts we need
	if next != 0 {
		return next
	}
	// if chunk has no next chunk, but has a previous one, we assume the length of this one is same as the previous one
	if prev != 0 {
		return itgen.Ts + (itgen.Ts - prev)
	}
	// if a chunk has no next and no previous chunk we have to assume it's length is 0
	return itgen.Ts
}

// lastTs returns the last Ts of this metric cache
// since ranges are exclusive at the end this is actually the first Ts that is not cached
func (mc *CCacheMetric) lastTs() uint32 {
	mc.RLock()
	defer mc.RUnlock()
	return mc.nextTs(mc.keys[len(mc.keys)-1])
}

// seekAsc finds the t0 of the chunk that contains ts, by searching from old to recent
// if not found or can't be sure returns 0, false
// assumes we already have at least a read lock
func (mc *CCacheMetric) seekAsc(ts uint32) (uint32, bool) {
	log.WithFields(log.Fields{
		"timestamp": ts,
		"keys":      mc.keys,
	}).Debug("CCacheMetric: seekAsc(), seeking for timestamp in the keys")

	for i := 0; i < len(mc.keys) && mc.keys[i] <= ts; i++ {
		if mc.nextTs(mc.keys[i]) > ts {
			log.WithFields(log.Fields{
				"timestamp": ts,
				"first.key": mc.keys[i],
				"last.key":  mc.nextTs(mc.keys[i]),
			}).Debug("CCacheMetric: seekAsc(), seek found ts between keys")
			return mc.keys[i], true
		}
	}

	log.Debug("CCacheMetric: seekAsc(), unsuccessful")
	return 0, false
}

// seekDesc finds the t0 of the chunk that contains ts, by searching from recent to old
// if not found or can't be sure returns 0, false
// assumes we already have at least a read lock
func (mc *CCacheMetric) seekDesc(ts uint32) (uint32, bool) {
	log.WithFields(log.Fields{
		"timestamp": ts,
		"keys":      mc.keys,
	}).Debug("CCacheMetric: seekDesc(), seeking for timestamp in keys")

	for i := len(mc.keys) - 1; i >= 0 && mc.nextTs(mc.keys[i]) > ts; i-- {
		if mc.keys[i] <= ts {
			log.WithFields(log.Fields{
				"timestamp": ts,
				"first.key": mc.keys[i],
				"last.key":  mc.nextTs(mc.keys[i]),
			}).Debug("CCacheMetric: seekDesc(), seek found timestamp between keys")
			return mc.keys[i], true
		}
	}

	log.Debug("CCacheMetric: seekDesc() unsuccessful")
	return 0, false
}

func (mc *CCacheMetric) searchForward(ctx context.Context, metric schema.AMKey, from, until uint32, res *CCSearchResult) {
	ts, ok := mc.seekAsc(from)
	if !ok {
		return
	}

	// add all consecutive chunks to search results, starting at the one containing "from"
	for ; ts != 0; ts = mc.chunks[ts].Next {
		log.WithFields(log.Fields{
			"timestamp": ts,
		}).Debug("CCacheMetric: searchForward(), forward search adds chunk to start")
		res.Start = append(res.Start, mc.chunks[ts].Itgen)
		nextTs := mc.nextTs(ts)
		res.From = nextTs

		if nextTs >= until {
			res.Complete = true
			break
		}
		if mc.chunks[ts].Next != 0 && ts >= mc.chunks[ts].Next {
			log.WithFields(log.Fields{
				"metric":            metric,
				"from":              from,
				"until":             until,
				"current.timestamp": ts,
				"next.timestamp":    mc.chunks[ts].Next,
			}).Warn("CCacheMetric: searchForward(), suspected bug suppressed")
			span := opentracing.SpanFromContext(ctx)
			span.SetTag("searchForwardBug", true)
			searchFwdBug.Inc()
			break
		}
	}
}

func (mc *CCacheMetric) searchBackward(from, until uint32, res *CCSearchResult) {
	ts, ok := mc.seekDesc(until - 1)
	if !ok {
		return
	}

	for ; ts != 0; ts = mc.chunks[ts].Prev {
		if ts < from {
			break
		}

		log.WithFields(log.Fields{
			"timestamp": ts,
		}).Debug("CCacheMetric: searchBackward(), backward search adds chunk to end")
		res.End = append(res.End, mc.chunks[ts].Itgen)
		res.Until = ts
	}
}

// Search searches the CCacheMetric's data and returns a complete-as-possible CCSearchResult
//
// we first look for the chunks where the "from" and "until" ts are in.
// then we seek from the "from" towards "until"
// and add as many cunks as possible to the result, if this did not result
// in all chunks necessary to serve the request we do the same in the reverse
// order from "until" to "from"
// if the first seek in chronological direction already ends up with all the
// chunks we need to serve the request, the second one can be skipped.
//
// EXAMPLE:
// from ts:                    |
// until ts:                                                   |
// cache:            |---|---|---|   |   |   |   |   |---|---|---|---|---|---|
// chunks returned:          |---|                   |---|---|---|
func (mc *CCacheMetric) Search(ctx context.Context, metric schema.AMKey, res *CCSearchResult, from, until uint32) {
	mc.RLock()
	defer mc.RUnlock()

	if len(mc.chunks) < 1 {
		return
	}

	mc.searchForward(ctx, metric, from, until, res)
	if !res.Complete {
		mc.searchBackward(from, until, res)
	}

	if !res.Complete && res.From > res.Until {
		log.WithFields(log.Fields{
			"from":  res.From,
			"until": res.Until,
			"key":   mc.MKey.String(),
		}).Warn("CCacheMetric: Search(), found  from > until, printing chunks")
		mc.debugMetric(from-7200, until+7200)
		res.Complete = false
		res.Start = res.Start[:0]
		res.End = res.End[:0]
		res.From = from
		res.Until = until
	}
}

func (mc *CCacheMetric) debugMetric(from, until uint32) {
	log.WithFields(log.Fields{
		"from":  from,
		"until": until,
	}).Warn("CCacheMetric: debugMetric(), debugging metric between timestamps")
	for _, key := range mc.keys {
		if key >= from && key <= until {
			log.WithFields(log.Fields{
				"key":  key,
				"prev": mc.chunks[key].Prev,
				"next": mc.chunks[key].Next,
			}).Warn("CCacheMetric: debugMetric()")
		}
	}
	log.Warn("CCacheMetric: debugMetric() ------------------------\n")
}
