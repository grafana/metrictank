package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
)

// AggMetric takes in new values, updates the in-memory data and streams the points to aggregators
// it uses a circular buffer of chunks
// each chunk starts at their respective t0
// a t0 is a timestamp divisible by chunkSpan without a remainder (e.g. 2 hour boundaries)
// firstT0's data is held at index 0, indexes go up and wrap around from numChunks-1 to 0
// in addition, keep in mind that the last chunk is always a work in progress and not useable for aggregation
// AggMetric is concurrency-safe
type AggMetric struct {
	sync.RWMutex
	Key             string
	CurrentChunkPos int    // element in []Chunks that is active. All others are either finished or nil.
	NumChunks       uint32 // max size of the circular buffer
	ChunkSpan       uint32 // span of individual chunks in seconds
	Chunks          []*Chunk
	aggregators     []*Aggregator
}

// re-order the chunks with the oldest at start of the list and newest at the end.
// this is to support increasing the chunkspan at startup.
func (a *AggMetric) GrowNumChunks(numChunks uint32) {
	a.Lock()
	defer a.Unlock()
	a.NumChunks = numChunks

	if uint32(len(a.Chunks)) < a.NumChunks {
		// the circular buffer has never reached the original max size,
		// so it must still be ordered.
		return
	}

	orderdChunks := make([]*Chunk, len(a.Chunks))
	// start by writting the oldest chunk first, then each chunk in turn.
	pos := a.CurrentChunkPos - 1
	if pos < 0 {
		pos += len(a.Chunks)
	}
	for i := 0; i < len(a.Chunks); i++ {
		orderdChunks[i] = a.Chunks[pos]
		pos++
		if pos >= len(a.Chunks) {
			pos = 0
		}
	}
	a.Chunks = orderdChunks
	a.CurrentChunkPos = len(a.Chunks) - 1
	return
}

// NewAggMetric creates a metric with given key, it retains the given number of chunks each chunkSpan seconds long
// it optionally also creates aggregations with the given settings
func NewAggMetric(key string, chunkSpan, numChunks uint32, aggsetting ...aggSetting) *AggMetric {
	m := AggMetric{
		Key:       key,
		ChunkSpan: chunkSpan,
		NumChunks: numChunks,
		Chunks:    make([]*Chunk, 0, numChunks),
	}
	for _, as := range aggsetting {
		m.aggregators = append(m.aggregators, NewAggregator(key, as.span, as.chunkSpan, as.numChunks))
	}

	return &m
}

func (a *AggMetric) getChunk(pos int) *Chunk {
	if pos >= len(a.Chunks) {
		return nil
	}
	return a.Chunks[pos]
}

func (a *AggMetric) GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (uint32, []Iter) {
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
			case consolidation.Last:
				return a.lstMetric.Get(from, to)
			case consolidation.Min:
				return a.minMetric.Get(from, to)
			case consolidation.Max:
				return a.maxMetric.Get(from, to)
			case consolidation.Sum:
				return a.sumMetric.Get(from, to)
			}
			panic(fmt.Sprintf("AggMetric.GetAggregated(): unknown consolidator %q", consolidator))
			// note: no way to access sosMetric yet
		}
	}
	panic(fmt.Sprintf("GetAggregated called with unknown aggSpan %d", aggSpan))
}

// Get all data between the requested time ranges. From is inclusive, to is exclusive. from <= x < to
// more data then what's requested may be included
// also returns oldest point we have, so that if your query needs data before it, the caller knows when to query cassandra
func (a *AggMetric) Get(from, to uint32) (uint32, []Iter) {
	log.Debug("AggMetric %s Get(): %d - %d (%s - %s) span:%ds", a.Key, from, to, TS(from), TS(to), to-from-1)
	if from >= to {
		panic("invalid request. to must > from")
	}
	a.RLock()
	defer a.RUnlock()

	newestChunk := a.getChunk(a.CurrentChunkPos)

	if newestChunk == nil {
		// we dont have any data yet.
		log.Debug("AggMetric %s Get(): no data for requested range.", a.Key)
		return math.MaxInt32, make([]Iter, 0)
	}
	if from >= newestChunk.T0+a.ChunkSpan {
		// we have no data in the requested range.
		log.Debug("AggMetric %s Get(): no data for requested range.", a.Key)
		return math.MaxInt32, make([]Iter, 0)
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
		return math.MaxInt32, make([]Iter, 0)
	}

	if to <= oldestChunk.T0 {
		// the requested time range ends before any data we have.
		log.Debug("AggMetric %s Get(): no data for requested range", a.Key)
		return oldestChunk.T0, make([]Iter, 0)
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
			return to, make([]Iter, 0)
		}
	}

	// find the newest Chunk that "to" falls in.  If "to" extends to after the newest data
	// then just return the newest chunk.
	// some examples to clarify this more. assume newestChunk.T0 is at 120, then
	// for a to of 121 -> data upto (incl) 120 -> stay at this chunk, it has a point we need
	// for a to of 120 -> data upto (incl) 119 -> use older chunk
	// for a to of 119 -> data upto (incl) 118 -> use older chunk
	newestPos := a.CurrentChunkPos
	for to <= newestChunk.T0 {
		newestPos--
		if newestPos < 0 {
			newestPos += len(a.Chunks)
		}
		newestChunk = a.getChunk(newestPos)
		if newestChunk == nil {
			log.Error(3, "unexpected nil chunk.")
			return to, make([]Iter, 0)
		}
	}

	// now just start at oldestPos and move through the Chunks circular Buffer to newestPos
	iters := make([]Iter, 0, a.NumChunks)
	for oldestPos != newestPos {
		chunk := a.getChunk(oldestPos)
		iters = append(iters, NewIter(chunk.Iter(), "mem %s", chunk))
		oldestPos++
		if oldestPos >= int(a.NumChunks) {
			oldestPos = 0
		}
	}
	// add the last chunk
	chunk := a.getChunk(oldestPos)
	iters = append(iters, NewIter(chunk.Iter(), "mem %s", chunk))

	return oldestChunk.T0, iters
}

// this function must only be called while holding the lock
func (a *AggMetric) addAggregators(ts uint32, val float64) {
	for _, agg := range a.aggregators {
		log.Debug("AggMetric %s pushing %d,%f to aggregator %d", a.Key, ts, val, agg.span)
		agg.Add(ts, val)
	}
}

func (a *AggMetric) Persist(c *Chunk) {
	log.Debug("AggMetric %s Persist(): starting to save %v", a.Key, c)
	data := c.Series.Bytes()
	chunkSizeAtSave.Value(int64(len(data)))

	version := FormatStandardGoTsz
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint8(version))
	if err != nil {
		// TODO
	}
	_, err = buf.Write(data)
	if err != nil {
		// TODO
	}
	err = InsertChunk(a.Key, c.T0, buf.Bytes(), *metricTTL)
	if err == nil {
		a.Lock()
		c.Saved = true
		a.Unlock()
		log.Debug("AggMetric %s Persist(): save complete. %v", a.Key, c)
		chunkSaveOk.Inc(1)
	} else {
		log.Error(1, "AggMetric %s Persist(): failed to save %v to cassandra. %v, %s", a.Key, c, err)
		chunkSaveFail.Inc(1)
		// TODO
	}
}

// don't ever call with a ts of 0, cause we use 0 to mean not initialized!
func (a *AggMetric) Add(ts uint32, val float64) {
	a.Lock()
	defer a.Unlock()

	t0 := ts - (ts % a.ChunkSpan)

	currentChunk := a.getChunk(a.CurrentChunkPos)
	if currentChunk == nil {
		chunkCreate.Inc(1)
		// no data has been added to this metric at all.
		a.Chunks = append(a.Chunks, NewChunk(t0))

		if err := a.Chunks[0].Push(ts, val); err != nil {
			panic(fmt.Sprintf("FATAL ERROR: this should never happen. Pushing initial value <%d,%f> to new chunk at pos 0 failed: %q", ts, val, err))
		}

		log.Debug("AggMetric %s Add(): created first chunk with first point: %v", a.Key, a.Chunks[0])
	} else if t0 == currentChunk.T0 {
		if currentChunk.Saved {
			//TODO(awoods): allow the chunk to be re-opened.
			log.Error(3, "cant write to chunk that has already been saved. %s T0:%d", a.Key, currentChunk.T0)
			return
		}
		// last prior data was in same chunk as new point
		if err := a.Chunks[a.CurrentChunkPos].Push(ts, val); err != nil {
			log.Error(3, "failed to add metric to chunk for %s. %s", a.Key, err)
			return
		}
		log.Debug("AggMetric %s Add(): pushed new value to last chunk: %v", a.Key, a.Chunks[0])
	} else if t0 < currentChunk.T0 {
		log.Error(3, "Point at %d has t0 %d, goes back into previous chunk. CurrentChunk t0: %d, LastTs: %d", ts, t0, currentChunk.T0, currentChunk.LastTs)
		return
	} else {
		currentChunk.Finish()
		go a.Persist(currentChunk)

		a.CurrentChunkPos++
		if a.CurrentChunkPos >= int(a.NumChunks) {
			a.CurrentChunkPos = 0
		}

		chunkCreate.Inc(1)
		var msg string
		if len(a.Chunks) < int(a.NumChunks) {
			msg = fmt.Sprintf("added new chunk to buffer. now %d chunks", a.CurrentChunkPos+1)
			a.Chunks = append(a.Chunks, NewChunk(t0))
		} else {
			chunkClear.Inc(1)
			totalPoints <- -1 * int(a.Chunks[a.CurrentChunkPos].NumPoints)
			msg = fmt.Sprintf("cleared chunk at %d of %d and replaced with new", a.CurrentChunkPos, len(a.Chunks))
			a.Chunks[a.CurrentChunkPos] = NewChunk(t0)
		}

		if err := a.Chunks[a.CurrentChunkPos].Push(ts, val); err != nil {
			panic(fmt.Sprintf("FATAL ERROR: this should never happen. Pushing initial value <%d,%f> to new chunk at pos %d failed: %q", ts, val, a.CurrentChunkPos, err))
		}
		log.Debug("AggMetric %s Add(): %s and added the new point: %s", a.Key, msg, a.Chunks[a.CurrentChunkPos])
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
		}
		// chunk has not been written to in a while. Lets persist it.
		log.Info("Found stale Chunk, persisting it to Cassandra. key: %s T0: %d", a.Key, currentChunk.T0)
		currentChunk.Finish()
		go a.Persist(currentChunk)
	}
	return false
}
