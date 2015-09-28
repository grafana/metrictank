package main

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgryski/go-tsz"
)

// the below is outdated, aggregators will now get their points streamed, no need to call get()
// however, we may still need to accept get queries here for aggregators, if we implement crash recovery by loading chunks from disk
// though we could also have the agg's maintain a write-ahead log, or refill aggregators from the metric's write ahead log.

// while AggMetric typically has 1 static chunkSpan (we decide what is the preferred way to encode the data),
// it must support different aggregator spans. in particular:
// 1. aggregator spans may be shorter (e.g. chunkSpan is 4hours but we collect aggregates on 5/10/30 minute level)
// 2. aggregator spans may be larger (e.g. chunkSpan is 2 hours but we decide to collect daily aggregates)
// we want to be able to keep multiple chunks per AggMetric, to satisfy (2) but also because it makes sense to be able
// to serve the data from RAM if we have RAM available.
// in addition, keep in mind that the last chunk is always a work in progress and not useable for aggregation
// so, numChunks must at least cover the highest aggregation interval + one additional chunk

type Chunk struct {
	*tsz.Series
	points uint32 // number of points in this chunk
}

func NewChunk(start uint32) *Chunk {
	return &Chunk{tsz.New(start), 0}
}

func (c *Chunk) Push(t uint32, v float64) *Chunk {
	c.Series.Push(t, v)
	c.points += 1
	return c
}

// responsible for taking in new values, updating the in-memory data
// and streaming the updates to aggregators
type AggMetric struct {
	sync.Mutex
	key         string
	lastTs      uint32   // last timestamp seen
	firstStart  uint32   // rounded to chunkSpan. denotes the very first start seen, ever, even if no longer in range
	lastStart   uint32   // rounded to chunkSpan. denotes last start seen.
	numChunks   uint32   // amount of chunks to keep
	chunkSpan   uint32   // span of individual chunks in seconds
	chunks      []*Chunk // a circular buffer of size numChunks. firstStart's data is at index 0, indexes go up and wrap around from numChunks-1 to 0
	aggregators []*Aggregator
}

// NewAggMetric creates a metric with given key, it retains the given number of chunks each chunkSpan seconds long
// it optionally also creates aggregations with the given settings
func NewAggMetric(key string, chunkSpan, numChunks uint32, aggsetting ...aggSetting) *AggMetric {
	m := AggMetric{
		key:       key,
		chunkSpan: chunkSpan,
		numChunks: numChunks,
		chunks:    make([]*Chunk, numChunks),
	}
	for _, as := range aggsetting {
		m.aggregators = append(m.aggregators, NewAggregator(key, as.span, as.chunkSpan, as.numChunks))
	}
	go m.stats()
	go m.trimOldData()
	return &m
}

func (a *AggMetric) stats() {
	for range time.Tick(time.Duration(1) * time.Second) {
		sum := 0
		a.Lock()
		for _, chunk := range a.chunks {
			if chunk != nil {
				sum += int(chunk.points)
			}
		}
		a.Unlock()
		points.Update(int64(sum))
	}
}

func (a *AggMetric) trimOldData() {
	a.Lock()
	//for t := range time.Tick(time.Duration(a.chunkSpan) * time.Second) {
	// Finish // it's ok to re-finish if already finished
	//	}
	a.Unlock()
}

// this function must only be called while holding the lock
func (a *AggMetric) indexFor(start uint32) uint32 {
	return ((start - a.firstStart) / a.chunkSpan) % a.numChunks
}

// using user input, which has to be verified, and could be any input
// from inclusive, to exclusive. just like slices
func (a *AggMetric) GetUnsafe(from, to uint32) ([]*tsz.Iter, error) {
	if from >= to {
		return nil, errors.New("invalid request. to must > from")
	}
	a.Lock()
	//log.Printf("GetUnsafe(%d, %d)\n", from, to)
	firstStart := from - (from % a.chunkSpan)
	lastStart := (to - 1) - ((to - 1) % a.chunkSpan)
	// we cannot satisfy data older than our retention
	oldestStartWeMayHave := a.lastStart - (a.numChunks-1)*a.chunkSpan
	if firstStart < oldestStartWeMayHave {
		firstStart = oldestStartWeMayHave
	}
	// no point in requesting data older then what we have. common shortly after server start
	if firstStart < a.firstStart {
		firstStart = a.firstStart
	}
	if lastStart > a.lastStart {
		lastStart = a.lastStart
	}

	defer a.Unlock()
	return a.get(firstStart, lastStart), nil
}

// using input from our software, which should already be solid.
// returns a range that includes the requested range, but typically more.
// from inclusive, to exclusive. just like slices
func (a *AggMetric) GetSafe(from, to uint32) []*tsz.Iter {
	if from >= to {
		panic("invalid request. to must > from")
	}
	a.Lock()
	firstStart := from - (from % a.chunkSpan)
	lastStart := (to - 1) - ((to - 1) % a.chunkSpan)
	aggFirstStart := int(a.lastStart) - int(a.numChunks*a.chunkSpan)
	// this can happen in contrived, testing scenarios that use very low timestamps
	if aggFirstStart < 0 {
		aggFirstStart = 0
	}
	if int(firstStart) < aggFirstStart {
		panic("requested a firstStart that is too old")
	}
	if lastStart > a.lastStart {
		panic(fmt.Sprintf("requested lastStart %d that doesn't exist yet. last is %d", lastStart, a.lastStart))
	}
	defer a.Unlock()
	return a.get(firstStart, lastStart)
}

// firstStart and lastStart must be aligned to marker intervals
func (a *AggMetric) get(firstStart, lastStart uint32) []*tsz.Iter {
	first := a.indexFor(firstStart)
	last := a.indexFor(lastStart)
	var data []*Chunk
	if last >= first {
		data = a.chunks[first : last+1]
	} else {
		numAtSliceEnd := (a.numChunks - first)
		numAtSliceBegin := last + 1
		num := numAtSliceEnd + numAtSliceBegin
		data = make([]*Chunk, 0, num)
		// add the values at the end of chunks slice first (they are first in time)
		for i := first; i < a.numChunks; i++ {
			data = append(data, a.chunks[i])
		}
		// then the values later in time, which are at the beginning of the slice
		for i := uint32(0); i <= last; i++ {
			data = append(data, a.chunks[i])
		}
	}
	iters := make([]*tsz.Iter, 0, len(data))
	for _, chunk := range data {
		if chunk != nil {
			iters = append(iters, chunk.Iter())
		}
	}
	return iters
}

// this function must only be called while holding the lock
func (a *AggMetric) addAggregators(ts uint32, val float64) {
	for _, agg := range a.aggregators {
		agg.Add(ts, val)
	}
}

// don't ever call with a ts of 0, cause we use 0 to mean not initialized!
func (a *AggMetric) Add(ts uint32, val float64) {
	a.Lock()
	defer a.Unlock()
	if ts <= a.lastTs {
		log.Printf("ERROR: ts %d <= last seen ts %d. can only append newer data", ts, a.lastTs)
		return
	}
	start := ts - (ts % a.chunkSpan)

	if a.lastTs == 0 {
		// we're adding first point ever..
		a.firstStart, a.lastStart = start, start
		a.lastTs = ts
		a.chunks[0] = NewChunk(start).Push(ts, val)
		a.addAggregators(ts, val)
		return
	}

	if start == a.lastStart {
		// last prior data was in same chunk as new point
		a.chunks[a.indexFor(start)].Push(ts, val)
	} else {
		// the point needs a newer chunk than points we've seen before
		a.chunks[a.indexFor(a.lastStart)].Finish()

		// TODO: create empty series in between, if there's a gap.

		a.chunks[a.indexFor(start)] = NewChunk(start).Push(ts, val)
		a.lastStart = start
		a.lastTs = ts
	}
	a.addAggregators(ts, val)
}
