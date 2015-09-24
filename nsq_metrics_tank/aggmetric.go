package main

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/dgryski/go-tsz"
)

// while AggMetric typically has 1 static chunkSpan (we decide what is the preferred way to encode the data),
// it must support different aggregator spans. in particular:
// 1. aggregator spans may be shorter (e.g. chunkSpan is 4hours but we collect aggregates on 5/10/30 minute level)
// 2. aggregator spans may be larger (e.g. chunkSpan is 2 hours but we decide to collect daily aggregates)
// we want to be able to keep multiple chunks per AggMetric, to satisfy (2) but also because it makes sense to be able
// to serve the data from RAM if we have RAM available.
// in addition, keep in mind that the last chunk is always a work in progress and not useable for aggregation
// so, numChunks must at least cover the highest aggregation interval + one additional chunk

// responsible for taking in new values, updating the in-memory data
// and informing aggregators when their periods have lapsed
type AggMetric struct {
	sync.Mutex
	key         string
	lastTs      uint32        // last timestamp seen
	firstStart  uint32        // rounded to chunkSpan. denotes the very first start seen, ever, even if no longer in range
	lastStart   uint32        // rounded to chunkSpan. denotes last start seen.
	numChunks   uint32        // amount of chunks to keep
	chunkSpan   uint32        // span of individual chunks in seconds
	chunks      []*tsz.Series // a circular buffer of size numChunks. firstStart's data is at index 0, indexes go up and wrap around from numChunks-1 to 0
	aggregators []*Aggregator
}

func NewAggMetric(key string, chunkSpan, numChunks uint32) *AggMetric {
	m := AggMetric{
		key:       key,
		chunkSpan: chunkSpan,
		numChunks: numChunks,
		chunks:    make([]*tsz.Series, numChunks),
	}
	return &m
}

// using user input, which has to be verified, and could be any input
// from inclusive, to exclusive. just like slices
func (a *AggMetric) GetUnsafe(from, to uint32) ([]*tsz.Iter, error) {
	if from >= to {
		return nil, errors.New("invalid request. to must > from")
	}
	a.Lock()
	log.Printf("GetUnsafe(%d, %d)\n", from, to)
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
	// this can happen in contrived, testing scenarios
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
	log.Printf("get(%d, %d)\n", firstStart, lastStart)
	first := ((firstStart - a.firstStart) / a.chunkSpan) % a.numChunks
	last := ((lastStart - a.firstStart) / a.chunkSpan) % a.numChunks
	log.Printf("first chunk: %d, last chunk: %d\n", first, last)
	var data []*tsz.Series
	if last >= first {
		data = a.chunks[first : last+1]
	} else {
		//     the values at the end + values at the beginning
		num := (a.numChunks - first) + last + 1
		data = make([]*tsz.Series, 0, num)
		// at the values at the end of chunks slice first (they are first in time)
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
			log.Println("> chunk not nil. adding iter")
			iters = append(iters, chunk.Iter())
		} else {
			log.Println("> chunk nil. skipping")
		}
	}
	return iters
}

// this function must only be called while holding the lock
func (a *AggMetric) signalAggregators(ts uint32) {
	for _, agg := range a.aggregators {
		agg.Signal(a, ts)
	}
}

func (a *AggMetric) Add(ts uint32, val float64) {
	a.Lock()
	defer a.Unlock()
	if ts <= a.lastTs {
		log.Println("ERROR: ts <= last seen ts")
		return
	}
	start := ts - (ts % a.chunkSpan)
	if a.key == "litmus.localhost.dev1.dns.ok_state" {
		log.Println(a.key, "adding", val, ts, "start is", start)
	}

	// if we're adding first point ever..
	if a.firstStart == 0 {
		a.firstStart = start
		a.lastStart = start
		series := tsz.New(start)
		series.Push(ts, val)
		a.chunks[0] = series
		log.Printf("chunk 0 for %d", start)
		a.signalAggregators(ts)
		return
	}

	if start == a.lastStart {
		// last prior data was in same chunk as new point
		index := ((start - a.firstStart) / a.chunkSpan) % a.numChunks
		a.chunks[index].Push(ts, val)
	} else {
		// the point needs a newer chunk than points we've seen before
		// start is higher than lastStart, because we already checked the ts

		// finish last chunk
		lastIndex := ((a.lastStart - a.firstStart) / a.chunkSpan) % a.numChunks
		a.chunks[lastIndex].Finish()

		// create new chunk
		series := tsz.New(start)
		series.Push(ts, val)
		newIndex := ((start - a.firstStart) / a.chunkSpan) % a.numChunks
		a.chunks[newIndex] = series
		log.Printf("chunk %d for %d", newIndex, start)

		// create empty series in between, if there's a gap. TODO

		a.lastStart = start
	}
	a.signalAggregators(ts)
}
