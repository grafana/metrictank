package mdata

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"github.com/raintank/raintank-metric/metric_tank/iter"
	"github.com/raintank/raintank-metric/metric_tank/mdata/chunk"
)

// AggMetric takes in new values, updates the in-memory data and streams the points to aggregators
// it uses a circular buffer of chunks
// each chunk starts at their respective t0
// a t0 is a timestamp divisible by chunkSpan without a remainder (e.g. 2 hour boundaries)
// firstT0's data is held at index 0, indexes go up and wrap around from numChunks-1 to 0
// in addition, keep in mind that the last chunk is always a work in progress and not useable for aggregation
// AggMetric is concurrency-safe
type AggMetric struct {
	store Store
	sync.RWMutex
	Key             string
	CurrentChunkPos int    // element in []Chunks that is active. All others are either finished or nil.
	NumChunks       uint32 // max size of the circular buffer
	ChunkSpan       uint32 // span of individual chunks in seconds
	Chunks          []*chunk.Chunk
	aggregators     []*Aggregator
	firstChunkT0    uint32
	ttl             uint32
}

// NewAggMetric creates a metric with given key, it retains the given number of chunks each chunkSpan seconds long
// it optionally also creates aggregations with the given settings
func NewAggMetric(store Store, key string, chunkSpan, numChunks uint32, ttl uint32, aggsetting ...AggSetting) *AggMetric {
	m := AggMetric{
		store:     store,
		Key:       key,
		ChunkSpan: chunkSpan,
		NumChunks: numChunks,
		Chunks:    make([]*chunk.Chunk, 0, numChunks),
		ttl:       ttl,
	}
	for _, as := range aggsetting {
		m.aggregators = append(m.aggregators, NewAggregator(store, key, as.Span, as.ChunkSpan, as.NumChunks, as.Ttl))
	}

	return &m
}

// Sync the saved state of a chunk by its T0.
func (a *AggMetric) SyncChunkSaveState(ts uint32) {
	a.Lock()
	defer a.Unlock()
	chunk := a.getChunkByT0(ts)
	if chunk != nil {
		if LogLevel < 2 {
			log.Debug("AM marking chunk %s:%d as saved.", a.Key, chunk.T0)
		}
		chunk.Saved = true
	}
}

/* Get a chunk by its T0.  It is expected that the caller has acquired a.Lock()*/
func (a *AggMetric) getChunkByT0(ts uint32) *chunk.Chunk {
	// we have no chunks.
	if len(a.Chunks) == 0 {
		return nil
	}

	currentT0 := a.Chunks[a.CurrentChunkPos].T0

	if ts == currentT0 {
		//found our chunk.
		return a.Chunks[a.CurrentChunkPos]
	}

	// requested Chunk is not in our dataset.
	if ts > currentT0 {
		return nil
	}

	// requested Chunk is not in our dataset.
	if len(a.Chunks) == 1 {
		return nil
	}

	// calculate the number of chunks ago our requested T0 is,
	// assuming that chunks are sequential.
	chunksAgo := int((currentT0 - ts) / a.ChunkSpan)

	numChunks := len(a.Chunks)
	oldestPos := a.CurrentChunkPos + 1
	if oldestPos >= numChunks {
		oldestPos = 0
	}

	var guess int

	if chunksAgo >= (numChunks - 1) {
		// set guess to the oldest chunk.
		guess = oldestPos
	} else {
		guess = a.CurrentChunkPos - chunksAgo
		if guess < 0 {
			guess += numChunks
		}
	}

	// we now have a good guess at which chunk position our requested TO is in.
	c := a.Chunks[guess]

	if c.T0 == ts {
		// found our chunk.
		return c
	}

	if ts > c.T0 {
		// we need to check newer chunks
		for c.T0 < currentT0 {
			guess += 1
			if guess >= numChunks {
				guess = 0
			}
			c = a.Chunks[guess]
			if c.T0 == ts {
				//found our chunk
				return c
			}
		}
	} else {
		// we need to check older chunks
		oldestT0 := a.Chunks[oldestPos].T0
		for c.T0 >= oldestT0 && c.T0 < currentT0 {
			guess -= 1
			if guess < 0 {
				guess += numChunks
			}
			c = a.Chunks[guess]
			if c.T0 == ts {
				//found or chunk.
				return c
			}
		}
	}
	// chunk not found.
	return nil
}

func (a *AggMetric) getChunk(pos int) *chunk.Chunk {
	if pos < 0 || pos >= len(a.Chunks) {
		panic(fmt.Sprintf("aggmetric %s queried for chunk %d out of %d chunks", a.Key, pos, len(a.Chunks)))
	}
	return a.Chunks[pos]
}

func (a *AggMetric) GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (uint32, []iter.Iter) {
	// no lock needed cause aggregators don't change at runtime
	for _, a := range a.aggregators {
		if a.span == aggSpan {
			switch consolidator {
			case consolidation.None:
				panic("cannot get an archive for no consolidation")
			case consolidation.Avg:
				panic("avg consolidator has no matching Archive(). you need sum and cnt")
			case consolidation.Cnt:
				return a.cntMetric.Get(from, to)
			case consolidation.Min:
				return a.minMetric.Get(from, to)
			case consolidation.Max:
				return a.maxMetric.Get(from, to)
			case consolidation.Sum:
				return a.sumMetric.Get(from, to)
			}
			panic(fmt.Sprintf("AggMetric.GetAggregated(): unknown consolidator %q", consolidator))
		}
	}
	panic(fmt.Sprintf("GetAggregated called with unknown aggSpan %d", aggSpan))
}

// Get all data between the requested time ranges. From is inclusive, to is exclusive. from <= x < to
// more data then what's requested may be included
// also returns oldest point we have, so that if your query needs data before it, the caller knows when to query cassandra
func (a *AggMetric) Get(from, to uint32) (uint32, []iter.Iter) {
	pre := time.Now()
	if LogLevel < 2 {
		log.Debug("AM %s Get(): %d - %d (%s - %s) span:%ds", a.Key, from, to, TS(from), TS(to), to-from-1)
	}
	if from >= to {
		panic("invalid request. to must > from")
	}
	a.RLock()
	defer a.RUnlock()

	if len(a.Chunks) == 0 {
		// we dont have any data yet.
		if LogLevel < 2 {
			log.Debug("AM %s Get(): no data for requested range.", a.Key)
		}
		return math.MaxInt32, make([]iter.Iter, 0)
	}

	newestChunk := a.getChunk(a.CurrentChunkPos)

	if from >= newestChunk.T0+a.ChunkSpan {
		// request falls entirely ahead of the data we have
		// this can happen in a few cases:
		// * queries for the most recent data, but our ingestion has fallen behind.
		//   we don't want such a degradation to cause a storm of cassandra queries
		//   we should just return an oldest value that is <= from so we don't hit cassandra
		//   but it doesn't have to be the real oldest value, so do whatever is cheap.
		// * data got once written by another node, but has been re-sending (perhaps fixed)
		//   to this node, but not yet up to the point it was previously sent. so we're
		//   only aware of older data and not the newer data in cassandra. this is unlikely
		//   and it's better to not serve this scenario well in favor of the above case.
		//   seems like a fair tradeoff anyway that you have to refill all the way first.
		if LogLevel < 2 {
			log.Debug("AM %s Get(): no data for requested range.", a.Key)
		}
		return from, make([]iter.Iter, 0)
	}

	// get the oldest chunk we have.
	// eg if we have 5 chunks, N is the current chunk and n-4 is the oldest chunk.
	// -----------------------------
	// | n-4 | n-3 | n-2 | n-1 | n |  CurrentChunkPos = 4
	// -----------------------------
	// -----------------------------
	// | n | n-4 | n-3 | n-2 | n-1 |  CurrentChunkPos = 0
	// -----------------------------
	// -----------------------------
	// | n-2 | n-1 | n | n-4 | n-3 |  CurrentChunkPos = 2
	// -----------------------------
	oldestPos := a.CurrentChunkPos + 1
	if oldestPos >= len(a.Chunks) {
		oldestPos = 0
	}

	oldestChunk := a.getChunk(oldestPos)
	if oldestChunk == nil {
		log.Error(3, "unexpected nil chunk.")
		return math.MaxInt32, make([]iter.Iter, 0)
	}

	// The first chunk is likely only a partial chunk. If we are not the primary node
	// we should not serve data from this chunk, and should instead get the chunk from cassandra.
	// if we are the primary node, then there is likely no data in Cassandra anyway.
	if !CluStatus.IsPrimary() && oldestChunk.T0 == a.firstChunkT0 {
		oldestPos++
		if oldestPos >= len(a.Chunks) {
			oldestPos = 0
		}
		oldestChunk = a.getChunk(oldestPos)
		if oldestChunk == nil {
			log.Error(3, "unexpected nil chunk.")
			return math.MaxInt32, make([]iter.Iter, 0)
		}
	}

	if to <= oldestChunk.T0 {
		// the requested time range ends before any data we have.
		if LogLevel < 2 {
			log.Debug("AM %s Get(): no data for requested range", a.Key)
		}
		return oldestChunk.T0, make([]iter.Iter, 0)
	}

	// Find the oldest Chunk that the "from" ts falls in.  If from extends before the oldest
	// chunk, then we just use the oldest chunk.
	for from >= oldestChunk.T0+a.ChunkSpan {
		oldestPos++
		if oldestPos >= len(a.Chunks) {
			oldestPos = 0
		}
		oldestChunk = a.getChunk(oldestPos)
		if oldestChunk == nil {
			log.Error(3, "unexpected nil chunk.")
			return to, make([]iter.Iter, 0)
		}
	}

	// find the newest Chunk that "to" falls in.  If "to" extends to after the newest data
	// then just return the newest chunk.
	// some examples to clarify this more. assume newestChunk.T0 is at 120, then
	// for a to of 121 -> data up to (incl) 120 -> stay at this chunk, it has a point we need
	// for a to of 120 -> data up to (incl) 119 -> use older chunk
	// for a to of 119 -> data up to (incl) 118 -> use older chunk
	newestPos := a.CurrentChunkPos
	for to <= newestChunk.T0 {
		newestPos--
		if newestPos < 0 {
			newestPos += len(a.Chunks)
		}
		newestChunk = a.getChunk(newestPos)
		if newestChunk == nil {
			log.Error(3, "unexpected nil chunk.")
			return to, make([]iter.Iter, 0)
		}
	}

	// now just start at oldestPos and move through the Chunks circular Buffer to newestPos
	iters := make([]iter.Iter, 0, a.NumChunks)
	for oldestPos != newestPos {
		chunk := a.getChunk(oldestPos)
		iters = append(iters, iter.New(chunk.Iter(), false))
		oldestPos++
		if oldestPos >= len(a.Chunks) {
			oldestPos = 0
		}
	}
	// add the last chunk
	chunk := a.getChunk(oldestPos)
	iters = append(iters, iter.New(chunk.Iter(), false))

	memToIterDuration.Value(time.Now().Sub(pre))
	return oldestChunk.T0, iters
}

// this function must only be called while holding the lock
func (a *AggMetric) addAggregators(ts uint32, val float64) {
	for _, agg := range a.aggregators {
		if LogLevel < 2 {
			log.Debug("AM %s pushing %d,%f to aggregator %d", a.Key, ts, val, agg.span)
		}
		agg.Add(ts, val)
	}
}

// write a chunk to peristant storage. This should only be called while holding a.Lock()
func (a *AggMetric) persist(pos int) {
	pre := time.Now()
	chunk := a.Chunks[pos]
	chunk.Finish()
	if !CluStatus.IsPrimary() {
		if LogLevel < 2 {
			log.Debug("AM persist(): node is not primary, not saving chunk.")
		}
		return
	}

	// create an array of chunks that need to be sent to the writeQueue.
	pending := make([]*ChunkWriteRequest, 1)
	// add the current chunk to the list of chunks to send to the writeQueue
	pending[0] = &ChunkWriteRequest{
		key:       a.Key,
		chunk:     chunk,
		ttl:       a.ttl,
		timestamp: time.Now(),
	}

	// if we recently became the primary, there may be older chunks
	// that the old primary did not save.  We should check for those
	// and save them.
	previousPos := pos - 1
	if previousPos < 0 {
		previousPos += len(a.Chunks)
	}
	previousChunk := a.Chunks[previousPos]
	for (previousChunk.T0 < chunk.T0) && !previousChunk.Saved && !previousChunk.Saving {
		if LogLevel < 2 {
			log.Debug("AM persist(): old chunk needs saving. Adding %s:%d to writeQueue", a.Key, previousChunk.T0)
		}
		pending = append(pending, &ChunkWriteRequest{
			key:       a.Key,
			chunk:     previousChunk,
			ttl:       a.ttl,
			timestamp: time.Now(),
		})
		previousPos--
		if previousPos < 0 {
			previousPos += len(a.Chunks)
		}
		previousChunk = a.Chunks[previousPos]
	}

	if LogLevel < 2 {
		log.Debug("AM persist(): sending %d chunks to write queue", len(pending))
	}

	pendingChunk := len(pending) - 1

	// if the store blocks,
	// the calling function will block waiting for persist() to complete.
	// This is intended to put backpressure on our message handlers so
	// that they stop consuming messages, leaving them to buffer at
	// the message bus. The "pending" array of chunks are processed
	// last-to-first ensuring that older data is added to the store
	// before newer data.
	for pendingChunk >= 0 {
		if LogLevel < 2 {
			log.Debug("AM persist(): adding chunk %d/%d (%s:%d) to write queue.", pendingChunk, len(pending), a.Key, chunk.T0)
		}
		a.store.Add(pending[pendingChunk])
		pending[pendingChunk].chunk.Saving = true
		pendingChunk--
	}
	persistDuration.Value(time.Now().Sub(pre))
	return
}

// don't ever call with a ts of 0, cause we use 0 to mean not initialized!
func (a *AggMetric) Add(ts uint32, val float64) {
	a.Lock()
	defer a.Unlock()

	t0 := ts - (ts % a.ChunkSpan)

	if len(a.Chunks) == 0 {
		chunkCreate.Inc(1)
		// no data has been added to this metric at all.
		a.Chunks = append(a.Chunks, chunk.New(t0))

		// The first chunk is typically going to be a partial chunk
		// so we keep a record of it.
		a.firstChunkT0 = t0

		if err := a.Chunks[0].Push(ts, val); err != nil {
			panic(fmt.Sprintf("FATAL ERROR: this should never happen. Pushing initial value <%d,%f> to new chunk at pos 0 failed: %q", ts, val, err))
		}

		log.Debug("AM %s Add(): created first chunk with first point: %v", a.Key, a.Chunks[0])
	}

	currentChunk := a.getChunk(a.CurrentChunkPos)

	if t0 == currentChunk.T0 {
		if currentChunk.Saved {
			//TODO(awoods): allow the chunk to be re-opened.
			log.Error(3, "cant write to chunk that has already been saved. %s T0:%d", a.Key, currentChunk.T0)
			return
		}
		// last prior data was in same chunk as new point
		if err := a.Chunks[a.CurrentChunkPos].Push(ts, val); err != nil {
			log.Debug("AM failed to add metric to chunk for %s. %s", a.Key, err)
			metricsTooOld.Inc(1)
			return
		}
		log.Debug("AM %s Add(): pushed new value to last chunk: %v", a.Key, a.Chunks[0])
	} else if t0 < currentChunk.T0 {
		log.Debug("AM Point at %d has t0 %d, goes back into previous chunk. CurrentChunk t0: %d, LastTs: %d", ts, t0, currentChunk.T0, currentChunk.LastTs)
		metricsTooOld.Inc(1)
		return
	} else {
		// persist the chunk. If the writeQueue is full, then this will block.
		a.persist(a.CurrentChunkPos)

		a.CurrentChunkPos++
		if a.CurrentChunkPos >= int(a.NumChunks) {
			a.CurrentChunkPos = 0
		}

		chunkCreate.Inc(1)
		if len(a.Chunks) < int(a.NumChunks) {
			a.Chunks = append(a.Chunks, chunk.New(t0))
			if err := a.Chunks[a.CurrentChunkPos].Push(ts, val); err != nil {
				panic(fmt.Sprintf("FATAL ERROR: this should never happen. Pushing initial value <%d,%f> to new chunk at pos %d failed: %q", ts, val, a.CurrentChunkPos, err))
			}
			log.Debug("AM %s Add(): added new chunk to buffer. now %d chunks. and added the new point: %s", a.Key, a.CurrentChunkPos+1, a.Chunks[a.CurrentChunkPos])
		} else {
			chunkClear.Inc(1)
			a.Chunks[a.CurrentChunkPos].Clear()
			a.Chunks[a.CurrentChunkPos] = chunk.New(t0)
			if err := a.Chunks[a.CurrentChunkPos].Push(ts, val); err != nil {
				panic(fmt.Sprintf("FATAL ERROR: this should never happen. Pushing initial value <%d,%f> to new chunk at pos %d failed: %q", ts, val, a.CurrentChunkPos, err))
			}
			log.Debug("AM %s Add(): cleared chunk at %d of %d and replaced with new. and added the new point: %s", a.Key, a.CurrentChunkPos, len(a.Chunks), a.Chunks[a.CurrentChunkPos])
		}

	}
	a.addAggregators(ts, val)
}

func (a *AggMetric) GC(chunkMinTs, metricMinTs uint32) bool {
	a.Lock()
	defer a.Unlock()
	currentChunk := a.getChunk(a.CurrentChunkPos)
	if currentChunk == nil {
		return false
	}

	if currentChunk.LastWrite < chunkMinTs {
		if currentChunk.Saved {
			// already saved. lets check if we should just delete the metric from memory.
			if currentChunk.LastWrite < metricMinTs {
				return true
			}
		} else {
			// chunk has not been written to in a while. Lets persist it.
			log.Info("Found stale Chunk, persisting it to Cassandra. key: %s T0: %d", a.Key, currentChunk.T0)
			a.persist(a.CurrentChunkPos)
		}
	}
	return false
}
