package main

import (
	"bytes"
	"encoding/gob"
	"github.com/dgryski/go-tsz"
	"log"
	"sync"
	"time"
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
	Key         string
	FirstTs     uint32 // first timestamp from which we have seen data. (either a point ts during our first chunk, or the t0 of a chunk if we've had to drop old chunks)
	LastTs      uint32 // last timestamp seen
	FirstT0     uint32 // first t0 seen, even if no longer in range
	LastT0      uint32 // last t0 seen
	NumChunks   uint32 // size of the circular buffer
	ChunkSpan   uint32 // span of individual chunks in seconds
	Chunks      []*Chunk
	aggregators []*Aggregator
}

type aggMetricOnDisk struct {
	Key       string
	FirstTs   uint32 // first timestamp from which we have seen data. (either a point ts during our first chunk, or the t0 of a chunk if we've had to drop old chunks)
	LastTs    uint32 // last timestamp seen
	FirstT0   uint32 // first t0 seen, even if no longer in range
	LastT0    uint32 // last t0 seen
	NumChunks uint32 // size of the circular buffer
	ChunkSpan uint32 // span of individual chunks in seconds
	Chunks    []*Chunk
}

func (a AggMetric) GobEncode() ([]byte, error) {
	log.Println("marshaling AggMetric to Binary")
	aOnDisk := aggMetricOnDisk{
		Key:       a.Key,
		FirstTs:   a.FirstTs,
		LastTs:    a.LastTs,
		FirstT0:   a.FirstT0,
		NumChunks: a.NumChunks,
		ChunkSpan: a.ChunkSpan,
		Chunks:    a.Chunks,
	}
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(aOnDisk)

	return b.Bytes(), err
}

func (a *AggMetric) GobDecode(data []byte) error {
	log.Println("unmarshaling AggMetric to Binary")
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	aOnDisk := &aggMetricOnDisk{}
	err := dec.Decode(aOnDisk)
	if err != nil {
		return err
	}
	a.Key = aOnDisk.Key
	a.FirstTs = aOnDisk.FirstTs
	a.LastTs = aOnDisk.LastTs
	a.FirstT0 = aOnDisk.FirstT0
	a.LastT0 = aOnDisk.LastT0
	a.NumChunks = aOnDisk.NumChunks
	a.ChunkSpan = aOnDisk.ChunkSpan
	a.Chunks = aOnDisk.Chunks
	return nil
}

// NewAggMetric creates a metric with given key, it retains the given number of chunks each chunkSpan seconds long
// it optionally also creates aggregations with the given settings
func NewAggMetric(key string, chunkSpan, numChunks uint32, aggsetting ...aggSetting) *AggMetric {
	m := AggMetric{
		Key:       key,
		ChunkSpan: chunkSpan,
		NumChunks: numChunks,
		Chunks:    make([]*Chunk, numChunks),
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
		a.Lock()
		for _, chunk := range a.Chunks {
			if chunk != nil {
				sum += int(chunk.NumPoints)
			}
		}
		a.Unlock()
		points.Update(int64(sum))
	}
}

// this function must only be called while holding the lock
func (a *AggMetric) indexFor(t0 uint32) uint32 {
	return ((t0 - a.FirstT0) / a.ChunkSpan) % a.NumChunks
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
	firstT0 := from - (from % a.ChunkSpan)
	lastT0 := (to - 1) - ((to - 1) % a.ChunkSpan)
	// we cannot satisfy data older than our retention
	// note lastT0 will be > 0 because this AggMetric can only exist by adding at least 1 point
	// we could go under 0 in contrived, testing scenarios that use very low timestamps
	tmp := maxInt(int(a.LastT0)-int((a.NumChunks-1)*a.ChunkSpan), 0)
	oldestT0WeMayHave := uint32(tmp)

	// don't request data older than what we have (common shortly after server restart) or possibly could have
	firstT0 = max(max(firstT0, oldestT0WeMayHave), a.FirstT0)

	lastT0 = min(lastT0, a.LastT0)

	oldest := min(max(oldestT0WeMayHave, serverStart), a.FirstTs)

	return oldest, a.get(firstT0, lastT0)
}

// remember: firstT0 and lastT0 must be cleanly divisible by chunkSpan!
func (a *AggMetric) get(firstT0, lastT0 uint32) []*tsz.Iter {
	first := a.indexFor(firstT0)
	last := a.indexFor(lastT0)
	var data []*Chunk
	if last >= first {
		data = a.Chunks[first : last+1]
	} else {
		numAtSliceEnd := (a.NumChunks - first)
		numAtSliceBegin := last + 1
		num := numAtSliceEnd + numAtSliceBegin
		data = make([]*Chunk, 0, num)
		// add the values at the end of chunks slice first (they are first in time)
		for i := first; i < a.NumChunks; i++ {
			data = append(data, a.Chunks[i])
		}
		// then the values later in time, which are at the beginning of the slice
		for i := uint32(0); i <= last; i++ {
			data = append(data, a.Chunks[i])
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
		log.Println("saving maybe  ", c)
		data := c.Series.Bytes()
		chunkSizeAtSave.Value(int64(len(data)))
		err := InsertMetric(a.Key, c.T0, data, *metricTTL)
		a.Lock()
		defer a.Unlock()
		if err == nil {
			c.Saved = true
			log.Println("save ok      ", c)
			chunkSaveOk.Inc(1)
		} else {
			log.Println("ERROR no save", c, err)
			chunkSaveFail.Inc(1)
			// TODO
		}
	}()
}

// don't ever call with a ts of 0, cause we use 0 to mean not initialized!
func (a *AggMetric) Add(ts uint32, val float64) {
	a.Lock()
	defer a.Unlock()
	if ts <= a.LastTs {
		log.Printf("ERROR: ts %d <= last seen ts %d. can only append newer data", ts, a.LastTs)
		return
	}
	t0 := ts - (ts % a.ChunkSpan)
	//log.Println("t0=", TS(t0), "adding", TS(ts), val)

	if a.LastTs == 0 {
		// we're adding first point ever..
		a.FirstT0, a.LastT0 = t0, t0
		a.FirstTs, a.LastTs = ts, ts
		chunkCreate.Inc(1)
		a.Chunks[0] = NewChunk(t0).Push(ts, val)
		log.Println("created ", a.Chunks[0])
		a.addAggregators(ts, val)
		return
	}

	if t0 == a.LastT0 {
		// last prior data was in same chunk as new point
		a.Chunks[a.indexFor(t0)].Push(ts, val)
	} else {
		// the point needs a newer chunk than points we've seen before
		prev := a.indexFor(a.LastT0)
		a.Chunks[prev].Finish()
		a.Persist(a.Chunks[prev])

		// TODO: create empty series in between, if there's a gap. -> actually no, we may just as well keep the old data
		// but we have to make sure we don't accidentally return the old data out of order

		now := a.indexFor(t0)
		if a.Chunks[now] != nil {
			chunkClear.Inc(1)
			log.Println("clearing ", a.Chunks[now])
		}
		chunkCreate.Inc(1)
		a.Chunks[now] = NewChunk(t0).Push(ts, val)
		log.Println("created ", a.Chunks[now])

		// update firstTs to oldest t0
		var found *Chunk
		if now > prev {
			for i := prev; i <= now && found == nil; i++ {
				if a.Chunks[i] != nil {
					found = a.Chunks[i]
				}
			}
		} else {
			for i := prev; i < uint32(len(a.Chunks)) && found == nil; i++ {
				if a.Chunks[i] != nil {
					found = a.Chunks[i]
				}
			}
			for i := uint32(0); i <= now && found == nil; i++ {
				if a.Chunks[i] != nil {
					found = a.Chunks[i]
				}
			}
		}
		a.FirstTs = found.T0

		a.LastT0 = t0
	}
	a.LastTs = ts
	a.addAggregators(ts, val)
}
