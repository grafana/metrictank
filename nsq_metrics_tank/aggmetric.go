package main

import (
	"log"
	"sync"
	"time"

	"github.com/dgryski/go-tsz"
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
	sync.Mutex
	key         string
	firstTs     uint32 // first timestamp from which we have seen data. (either a point ts during our first chunk, or the t0 of a chunk if we've had to drop old chunks)
	lastTs      uint32 // last timestamp seen
	firstT0     uint32 // first t0 seen, even if no longer in range
	lastT0      uint32 // last t0 seen
	numChunks   uint32 // size of the circular buffer
	chunkSpan   uint32 // span of individual chunks in seconds
	chunks      []*Chunk
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
	for range time.Tick(statsPeriod) {
		sum := 0
		a.Lock()
		for _, chunk := range a.chunks {
			if chunk != nil {
				sum += int(chunk.numPoints)
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
func (a *AggMetric) indexFor(t0 uint32) uint32 {
	return ((t0 - a.firstT0) / a.chunkSpan) % a.numChunks
}

// using user input, which has to be verified, and could be any input
// from inclusive, to exclusive. just like slices. may include more points that you asked for
// we also need to be accurately say from which point on we have data.
// that way you know if you should also query another datastore
// this is trickier than it sounds:
// 1) we can't just use server start, because older data may have come in with a bit of a delay
// 2) we can't just use oldest point that we still hold, because data may have arrived long after server start,
//    i.e. after a period of nothingness. and we know we would have had the data after start if it had come in
// 3) we can't just compute based on chunks how far back in time we would have had the data,
//    because this could be before server start, if invoked soon after starting.
// so we use a combination of all factors..
// note that this value may lie before the timestamp of actual data returned
func (a *AggMetric) Get(from, to uint32) (uint32, []*tsz.Iter) {
	if from >= to {
		panic("invalid request. to must > from")
	}
	a.Lock()
	defer a.Unlock()
	firstT0 := from - (from % a.chunkSpan)
	lastT0 := (to - 1) - ((to - 1) % a.chunkSpan)
	// we cannot satisfy data older than our retention
	// note lastT0 will be > 0 because this AggMetric can only exist by adding at least 1 point
	tmp := int(a.lastT0) - int((a.numChunks-1)*a.chunkSpan)
	// this can happen in contrived, testing scenarios that use very low timestamps
	if tmp < 0 {
		tmp = 0
	}
	oldestT0WeMayHave := uint32(tmp)
	if firstT0 < oldestT0WeMayHave {
		firstT0 = oldestT0WeMayHave
	}
	// no point in requesting data older then what we have. common shortly after server start
	if firstT0 < a.firstT0 {
		firstT0 = a.firstT0
	}
	if lastT0 > a.lastT0 {
		lastT0 = a.lastT0
	}

	oldest := oldestT0WeMayHave
	if oldest < serverStart {
		oldest = serverStart
	}
	if oldest > a.firstTs {
		oldest = a.firstTs
	}

	return oldest, a.get(firstT0, lastT0)
}

// remember: firstT0 and lastT0 must be cleanly divisible by chunkSpan!
func (a *AggMetric) get(firstT0, lastT0 uint32) []*tsz.Iter {
	first := a.indexFor(firstT0)
	last := a.indexFor(lastT0)
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

func (a *AggMetric) Persist(c *Chunk) {
	go func() {
		log.Println("if c* enabled, saving chunk", c)
		err := InsertMetric(a.key, c.t0, c.Series.Bytes())
		a.Lock()
		defer a.Unlock()
		if err == nil {
			c.saved = true
			log.Println("saved chunk", c)
		} else {
			log.Println("ERROR: could not save chunk", c, err)
			// TODO
		}
	}()
}

// don't ever call with a ts of 0, cause we use 0 to mean not initialized!
func (a *AggMetric) Add(ts uint32, val float64) {
	a.Lock()
	defer a.Unlock()
	if ts <= a.lastTs {
		log.Printf("ERROR: ts %d <= last seen ts %d. can only append newer data", ts, a.lastTs)
		return
	}
	t0 := ts - (ts % a.chunkSpan)
	log.Println("t0=", TS(t0), "adding", TS(ts), val)

	if a.lastTs == 0 {
		// we're adding first point ever..
		a.firstT0, a.lastT0 = t0, t0
		a.firstTs, a.lastTs = ts, ts
		a.chunks[0] = NewChunk(t0).Push(ts, val)
		a.addAggregators(ts, val)
		return
	}

	if t0 == a.lastT0 {
		// last prior data was in same chunk as new point
		a.chunks[a.indexFor(t0)].Push(ts, val)
	} else {
		// the point needs a newer chunk than points we've seen before
		prev := a.indexFor(a.lastT0)
		a.chunks[prev].Finish()
		a.Persist(a.chunks[prev])

		// TODO: create empty series in between, if there's a gap. -> actually no, we may just as well keep the old data
		// but we have to make sure we don't accidentally return the old data out of order

		now := a.indexFor(t0)
		a.chunks[now] = NewChunk(t0).Push(ts, val)

		// update firstTs to oldest t0
		var found *Chunk
		if now > prev {
			for i := prev; i <= now && found == nil; i++ {
				if a.chunks[i] != nil {
					found = a.chunks[i]
				}
			}
		} else {
			for i := prev; i < uint32(len(a.chunks)) && found == nil; i++ {
				if a.chunks[i] != nil {
					found = a.chunks[i]
				}
			}
			for i := uint32(0); i <= now && found == nil; i++ {
				if a.chunks[i] != nil {
					found = a.chunks[i]
				}
			}
		}
		a.firstTs = found.t0

		a.lastT0 = t0
	}
	a.lastTs = ts
	a.addAggregators(ts, val)
}
