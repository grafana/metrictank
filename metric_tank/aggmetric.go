package main

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"github.com/grafana/grafana/pkg/log"
	//"github.com/dgryski/go-tsz"
	"github.com/raintank/go-tsz"
)

var serverStart uint32
var statsPeriod time.Duration

func init() {
	serverStart = uint32(time.Now().Unix())
	statsPeriod = time.Duration(1) * time.Second
}

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
	NumChunks       uint32 // size of the circular buffer
	ChunkSpan       uint32 // span of individual chunks in seconds
	Chunks          []*Chunk
	aggregators     []*Aggregator
}

type aggMetricOnDisk struct {
	Key             string
	CurrentChunkPos int
	NumChunks       uint32
	ChunkSpan       uint32
	Chunks          []*Chunk
}

// re-order the chunks with the oldest at start of the list and newest at the end.
// this is to support increasing the chunkspan at startup.
func (a *AggMetric) GrowNumChunks(numChunks uint32) {
	a.Lock()
	defer a.Unlock()
	orderdChunks := make([]*Chunk, len(a.Chunks))
	if uint32(len(a.Chunks)) < a.NumChunks {
		// the circular buffer is not yet full.
		return
	}

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
	a.NumChunks = numChunks
	return
}

func (a AggMetric) GobEncode() ([]byte, error) {
	aOnDisk := aggMetricOnDisk{
		Key:             a.Key,
		CurrentChunkPos: a.CurrentChunkPos,
		NumChunks:       a.NumChunks,
		ChunkSpan:       a.ChunkSpan,
		Chunks:          a.Chunks,
	}
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(aOnDisk)

	return b.Bytes(), err
}

func (a *AggMetric) GobDecode(data []byte) error {
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	aOnDisk := &aggMetricOnDisk{}
	err := dec.Decode(aOnDisk)
	if err != nil {
		return err
	}
	a.Key = aOnDisk.Key

	a.NumChunks = aOnDisk.NumChunks
	a.ChunkSpan = aOnDisk.ChunkSpan
	a.Chunks = aOnDisk.Chunks
	//the current chunk is the last chunk.
	a.CurrentChunkPos = aOnDisk.CurrentChunkPos
	return nil
}

// NewAggMetric creates a metric with given key, it retains the given number of chunks each chunkSpan seconds long
// it optionally also creates aggregations with the given settings
func NewAggMetric(key string, chunkSpan, numChunks uint32, aggsetting ...aggSetting) *AggMetric {
	m := AggMetric{
		Key:       key,
		ChunkSpan: chunkSpan,
		NumChunks: numChunks,
		Chunks:    make([]*Chunk, 0),
	}
	for _, as := range aggsetting {
		m.aggregators = append(m.aggregators, NewAggregator(key, as.span, as.chunkSpan, as.numChunks))
	}
	go m.stats()
	return &m
}

func (a *AggMetric) stats() {
	for range time.Tick(statsPeriod) {
		sum := 0
		a.RLock()
		for _, chunk := range a.Chunks {
			if chunk != nil {
				sum += int(chunk.NumPoints)
			}
		}
		a.RUnlock()
		points.Update(int64(sum))
	}
}

func (a *AggMetric) getChunk(pos int) *Chunk {
	if pos >= len(a.Chunks) {
		return nil
	}
	return a.Chunks[pos]
}

// Get all data between the requested time rages. From is inclusive, to is exclusive. from <= x < to
func (a *AggMetric) Get(from, to uint32) (uint32, []*tsz.Iter) {
	log.Debug("GET: %s from: %d to:%d", a.Key, from, to)
	if from >= to {
		panic("invalid request. to must > from")
	}
	a.RLock()
	defer a.RUnlock()
	firstT0 := from - (from % a.ChunkSpan)
	lastT0 := (to - 1) - ((to - 1) % a.ChunkSpan)

	newestPos := a.CurrentChunkPos

	newestChunk := a.getChunk(a.CurrentChunkPos)

	if newestChunk == nil {
		// we dont have any data yet.
		log.Debug("no data for requested range.")
		return to, make([]*tsz.Iter, 0)
	}
	if firstT0 > newestChunk.T0 {
		// we have no data in the requested range.
		log.Debug("no data for requested range.")
		return to, make([]*tsz.Iter, 0)
	}

	// get the oldest chunk we have.
	// eg if we have 5 chunks, N is the current chunk and n-4 is the oldest chunk.
	// -----------------------------
	// | n | n-4 | n-3 | n-2 | n-1 |  CurrentChunkPos = 0
	// -----------------------------
	// -----------------------------
	// | n-2 | n-1 | n | n-4 | n-3 |  CurrentChunkPos = 3
	// -----------------------------
	oldestPos := a.CurrentChunkPos + 1
	if oldestPos >= len(a.Chunks) {
		oldestPos = 0
	}

	oldestChunk := a.getChunk(oldestPos)
	if oldestChunk == nil {
		log.Error(3, "unexpected nil chunk.")
		return to, make([]*tsz.Iter, 0)
	}

	if lastT0 < oldestChunk.T0 {
		// the requested time range ends before any data we have.
		log.Debug("no data for requested range")
		return to, make([]*tsz.Iter, 0)
	}

	// Find the oldest Chunk that the "from" ts falls in.  If from extends before the oldest
	// chunk, then we just use the oldest chunk.
	for firstT0 > oldestChunk.T0 {
		oldestPos++
		if oldestPos >= len(a.Chunks) {
			oldestPos = 0
		}
		oldestChunk = a.getChunk(oldestPos)
		if oldestChunk == nil {
			log.Error(3, "unexpected nil chunk.")
			return to, make([]*tsz.Iter, 0)
		}
	}

	firstT0 = oldestChunk.T0

	// find the newest Chunk that "to" falls in.  If "to" extends to after the newest data
	// then just return the newest chunk.
	for lastT0 < newestChunk.T0 {
		newestPos--
		if newestPos < 0 {
			newestPos += len(a.Chunks)
		}
		newestChunk = a.getChunk(newestPos)
		if newestChunk == nil {
			log.Error(3, "unexpected nil chunk.")
			return to, make([]*tsz.Iter, 0)
		}
	}

	// now just start at oldestPos and move through the Chunks circular Buffer to newestPos
	iters := make([]*tsz.Iter, 0)
	for oldestPos != newestPos {
		iters = append(iters, a.getChunk(oldestPos).Iter())
		oldestPos++
		if oldestPos >= int(a.NumChunks) {
			oldestPos = 0
		}
	}
	// add the last chunk
	iters = append(iters, a.getChunk(oldestPos).Iter())

	return firstT0, iters
}

// this function must only be called while holding the lock
func (a *AggMetric) addAggregators(ts uint32, val float64) {
	for _, agg := range a.aggregators {
		agg.Add(ts, val)
	}
}

func (a *AggMetric) Persist(c *Chunk) {
	go func() {
		log.Debug("starting to save %v", c)
		a.RLock()
		data := c.Series.Bytes()
		a.RUnlock()
		chunkSizeAtSave.Value(int64(len(data)))
		err := InsertMetric(a.Key, c.T0, data, *metricTTL)
		if err == nil {
			a.Lock()
			c.Saved = true
			a.Unlock()
			log.Debug("save complete. %v", c)
			chunkSaveOk.Inc(1)
		} else {
			log.Error(1, "failed to save metric to cassandra. %v, %s", c, err)
			chunkSaveFail.Inc(1)
			// TODO
		}
	}()
}

// don't ever call with a ts of 0, cause we use 0 to mean not initialized!
func (a *AggMetric) Add(ts uint32, val float64) {
	a.Lock()
	defer a.Unlock()

	t0 := ts - (ts % a.ChunkSpan)

	currentChunk := a.getChunk(a.CurrentChunkPos)
	if currentChunk == nil {
		chunkCreate.Inc(1)
		if len(a.Chunks) < int(a.NumChunks) {
			log.Debug("adding new chunk to cirular Buffer. now %d chunks", a.CurrentChunkPos+1)
			a.Chunks = append(a.Chunks, NewChunk(t0))
		} else {
			a.Chunks[a.CurrentChunkPos] = NewChunk(t0)
		}

		if err := a.Chunks[a.CurrentChunkPos].Push(ts, val); err != nil {
			log.Error(1, "failed to add metric to chunk. %s", err)
			return
		}

		log.Debug("created new chunk. %v", a.Chunks[0])
		a.addAggregators(ts, val)
		return
	}

	if t0 == currentChunk.T0 {

		if currentChunk.Saved {
			//TODO(awoods): allow the chunk to be re-opened.
			log.Error(3, "cant write to chunk that has already been saved. %s T0:%d", a.Key, currentChunk.T0)
			return
		}
		// last prior data was in same chunk as new point
		if err := a.Chunks[a.CurrentChunkPos].Push(ts, val); err != nil {
			log.Error(3, "failed to add metric to chunk. %s", err)
			return
		}
	} else {
		currentChunk.Finish()
		a.Persist(currentChunk)

		a.CurrentChunkPos++
		if a.CurrentChunkPos >= int(a.NumChunks) {
			a.CurrentChunkPos = 0
		}

		chunkCreate.Inc(1)
		if len(a.Chunks) < int(a.NumChunks) {
			log.Debug("adding new chunk to cirular Buffer. now %d chunks", a.CurrentChunkPos+1)
			a.Chunks = append(a.Chunks, NewChunk(t0))
		} else {
			chunkClear.Inc(1)
			log.Debug("numChunks: %d  currentPos: %d", len(a.Chunks), a.CurrentChunkPos)
			log.Debug("clearing chunk from circular buffer. %v", a.Chunks[a.CurrentChunkPos])
			a.Chunks[a.CurrentChunkPos] = NewChunk(t0)
		}
		log.Debug("created new chunk. %v", a.Chunks[a.CurrentChunkPos])

		if err := a.Chunks[a.CurrentChunkPos].Push(ts, val); err != nil {
			log.Error(1, "failed to push metric to chunk. %s", err)
			return
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

	if currentChunk.T0 < chunkMinTs {
		if currentChunk.Saved {
			// already saved. lets check if we should just delete the metric from memory.
			if currentChunk.T0 < metricMinTs {
				return true
			}
		}
		// chunk has not been written to in a while. Lets persist it.
		log.Info("Found stale Chunk, persisting it to Cassandra. key: %s T0: %d", a.Key, currentChunk.T0)
		currentChunk.Finish()
		a.Persist(currentChunk)
	}
	return false
}
