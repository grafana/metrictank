package mdata

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/metrictank/internal/cluster"
	"github.com/grafana/metrictank/internal/mdata/chunk"
	mdataerrors "github.com/grafana/metrictank/internal/mdata/errors"
	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/internal/util"
	"github.com/grafana/metrictank/pkg/conf"
	"github.com/grafana/metrictank/pkg/consolidation"
	log "github.com/sirupsen/logrus"
)

var ErrInvalidRange = errors.New("AggMetric: invalid range: from must be less than to")

// Flusher is used to determine what should happen when chunks are ready to flushed from AggMetrics
type Flusher interface {
	IsCacheable(metric schema.AMKey) bool
	Cache(metric schema.AMKey, prev uint32, itergen chunk.IterGen)
	Store(cwr *ChunkWriteRequest)
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
	flusher Flusher
	key     schema.AMKey
	rob     *ReorderBuffer
	chunks  []*chunk.Chunk

	// After NewAggMetric returns and thus before we start using the AggMetric, the contents of this slice (the pointers themselves)
	// remains constant. The Aggregators being pointed to, will have a AggMetric pointers. Those pointers will also remain constant
	// throughout the lifetime, and the AggMetric being pointed to has its own lock protection. However the Aggregator still has other
	// fields to hold the boundary and the data, and those are unprotected.  For read and write access to them,
	// you should use this AggMetric's RWMutex.
	aggregators     []*Aggregator
	currentChunkPos int    // Chunks[CurrentChunkPos] is active. Others are finished. Only valid when len(chunks) > 0, e.g. when data has been written (excl ROB data)
	chunkSpan       uint32 // span of individual chunks in seconds
	ingestFromT0    uint32
	futureTolerance uint32
	ttl             uint32
	lastSaveStart   uint32 // last chunk T0 that was added to the write Queue.
	lastWrite       uint32 // wall clock time of when last point was successfully added (possibly to the ROB)
	firstTs         uint32 // timestamp of first point seen
	dropFirstChunk  bool
}

// NewAggMetric creates a metric with given key, it retains the given number of chunks each chunkSpan seconds long
// it optionally also creates aggregations with the given settings
// the 0th retention is the native archive of this metric. if there's several others, we create aggregators, using agg.
// it's the callers responsibility to make sure agg is not nil in that case!
// If reorderWindow is greater than 0, a reorder buffer is enabled. In that case data points with duplicate timestamps
// the behavior is defined by reorderAllowUpdate
func NewAggMetric(flusher Flusher, key schema.AMKey, retentions conf.Retentions, reorderWindow, interval uint32, agg *conf.Aggregation, reorderAllowUpdate, dropFirstChunk bool, ingestFrom int64) *AggMetric {

	// note: during parsing of retentions, we assure there's at least 1.
	ret := retentions.Rets[0]

	m := AggMetric{
		flusher:         flusher,
		key:             key,
		chunks:          make([]*chunk.Chunk, 0, ret.NumChunks),
		chunkSpan:       ret.ChunkSpan,
		futureTolerance: uint32(ret.MaxRetention()) * uint32(futureToleranceRatio) / 100,
		ttl:             uint32(ret.MaxRetention()),
		// we set LastWrite here to make sure a new Chunk doesn't get immediately
		// garbage collected right after creating it, before we can push to it.
		lastWrite:      uint32(time.Now().Unix()),
		dropFirstChunk: dropFirstChunk,
	}
	if ingestFrom > 0 {
		// we only want to ingest data that will go into chunks with a t0 >= 'ingestFrom'.
		m.ingestFromT0 = AggBoundary(uint32(ingestFrom), ret.ChunkSpan)
	}
	if reorderWindow != 0 {
		m.rob = NewReorderBuffer(reorderWindow, interval, reorderAllowUpdate)
	}

	origSplits := strings.Split(retentions.Orig, ":")
	for i, ret := range retentions.Rets[1:] {
		retOrig := origSplits[i+1]
		m.aggregators = append(m.aggregators, NewAggregator(flusher, key, retOrig, ret, *agg, dropFirstChunk, ingestFrom))
	}

	return &m
}

// Sync the saved state of a chunk by its T0.
func (a *AggMetric) SyncChunkSaveState(ts uint32, sendPersist bool) ChunkSaveCallback {
	return func() {
		util.AtomicBumpUint32(&a.lastSaveStart, ts)

		log.Debugf("AM: metric %s at chunk T0=%d has been saved.", a.key, ts)
		if sendPersist {
			SendPersistMessage(a.key.String(), ts)
		}
	}
}

// Sync the saved state of a chunk by its T0.
func (a *AggMetric) SyncAggregatedChunkSaveState(ts uint32, consolidator consolidation.Consolidator, aggSpan uint32) {

	for _, a := range a.aggregators {
		if a.span == aggSpan {
			switch consolidator {
			case consolidation.None:
				panic("cannot get an archive for no consolidation")
			case consolidation.Avg:
				panic("avg consolidator has no matching Archive(). you need sum and cnt")
			case consolidation.Cnt:
				if a.cntMetric != nil {
					a.cntMetric.SyncChunkSaveState(ts, false)()
				}
				return
			case consolidation.Min:
				if a.minMetric != nil {
					a.minMetric.SyncChunkSaveState(ts, false)()
				}
				return
			case consolidation.Max:
				if a.maxMetric != nil {
					a.maxMetric.SyncChunkSaveState(ts, false)()
				}
				return
			case consolidation.Sum:
				if a.sumMetric != nil {
					a.sumMetric.SyncChunkSaveState(ts, false)()
				}
				return
			case consolidation.Lst:
				if a.lstMetric != nil {
					a.lstMetric.SyncChunkSaveState(ts, false)()
				}
				return
			default:
				panic(fmt.Sprintf("internal error: no such consolidator %q with span %d", consolidator, aggSpan))
			}
		}
	}
}

func (a *AggMetric) GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (Result, error) {
	for _, aggregator := range a.aggregators {
		if aggregator.span == aggSpan {
			var agg *AggMetric
			switch consolidator {
			case consolidation.None:
				err := errors.New("internal error: AggMetric.GetAggregated(): cannot get an archive for no consolidation")
				log.Errorf("AM: %s", err.Error())
				badConsolidator.Inc()
				return Result{}, err
			case consolidation.Avg:
				err := errors.New("internal error: AggMetric.GetAggregated(): avg consolidator has no matching Archive(). you need sum and cnt")
				log.Errorf("AM: %s", err.Error())
				badConsolidator.Inc()
				return Result{}, err
			case consolidation.Cnt:
				agg = aggregator.cntMetric
			case consolidation.Lst:
				agg = aggregator.lstMetric
			case consolidation.Min:
				agg = aggregator.minMetric
			case consolidation.Max:
				agg = aggregator.maxMetric
			case consolidation.Sum:
				agg = aggregator.sumMetric
			default:
				err := fmt.Errorf("internal error: AggMetric.GetAggregated(): unknown consolidator %q", consolidator)
				log.Errorf("AM: %s", err.Error())
				badConsolidator.Inc()
				return Result{}, err
			}
			if agg == nil {
				return Result{}, fmt.Errorf("consolidator %q not configured", consolidator)
			}

			// note: only the raw AggMetric potentially has a ROB
			// these children aggregator AggMetric never do, so result.Points is never set here
			result, err := agg.Get(from, to)
			if err != nil {
				return Result{}, err
			}

			var futurePoints []schema.Point

			a.RLock()
			defer a.RUnlock()

			if a.rob != nil {
				futurePoints = a.rob.Get()
				if len(futurePoints) > 0 {

					// There are 2 possibilities here:
					// A) The AggMetric is new: it may have all its points in the ROB, and no points may have been flushed yet to the chunks (no result.Iters).
					//    In this case, the oldest point comes from the ROB, so we update the Oldest field here.
					// B) Points have been flushed by the aggregator into the chunks:
					//    Because the aggregator (in Foresee but also in general) cannot accept points for boundaries that it has already flushed to the AggMetric's chunks,
					//    we know that the oldest point will always come from the chunks and the field is already properly set.

					if len(result.Iters) == 0 {
						result.Oldest = futurePoints[0].Ts
					}
				}
			}

			result.Points = aggregator.Foresee(consolidator, from, to, futurePoints)

			return result, nil
		}
	}
	err := fmt.Errorf("internal error: AggMetric.GetAggregated(): unknown aggSpan %d", aggSpan)
	log.Errorf("AM: %s", err.Error())
	badAggSpan.Inc()
	return Result{}, err
}

// Get all data between the requested time ranges. From is inclusive, to is exclusive. from <= x < to
// more data then what's requested may be included
// specifically, returns:
// * points from the ROB (if enabled)
// * iters from matching chunks
// * oldest point we have, so that if your query needs data before it, the caller knows when to query the store
func (a *AggMetric) Get(from, to uint32) (Result, error) {
	pre := time.Now()
	log.Debugf("AM: %s Get(): %d - %d (%s - %s) span:%ds", a.key, from, to, TS(from), TS(to), to-from-1)
	if from >= to {
		return Result{}, ErrInvalidRange
	}
	a.RLock()
	defer a.RUnlock()

	result := Result{
		Oldest: math.MaxInt32,
	}

	if a.rob != nil {
		result.Points = a.rob.Get()
		if len(result.Points) > 0 {
			result.Oldest = result.Points[0].Ts
			if result.Oldest <= from {
				return result, nil
			}
		}
	}

	if len(a.chunks) == 0 {
		// we dont have any data yet.
		log.Debugf("AM: %s Get(): no data for requested range.", a.key)
		return result, nil
	}

	newestChunk := a.chunks[a.currentChunkPos]

	if from >= newestChunk.Series.T0+a.chunkSpan {
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
		log.Debugf("AM: %s Get(): no data for requested range.", a.key)
		result.Oldest = from
		return result, nil
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
	oldestPos := a.currentChunkPos + 1
	if oldestPos >= len(a.chunks) {
		oldestPos = 0
	}

	oldestChunk := a.chunks[oldestPos]

	if to <= oldestChunk.Series.T0 {
		// the requested time range ends before any data we have.
		log.Debugf("AM: %s Get(): no data for requested range", a.key)
		if oldestChunk.First {
			result.Oldest = a.firstTs
		} else {
			result.Oldest = oldestChunk.Series.T0
		}
		return result, nil
	}

	// Find the oldest Chunk that the "from" ts falls in.  If from extends before the oldest
	// chunk, then we just use the oldest chunk.
	for from >= oldestChunk.Series.T0+a.chunkSpan {
		oldestPos++
		if oldestPos >= len(a.chunks) {
			oldestPos = 0
		}
		oldestChunk = a.chunks[oldestPos]
	}

	// find the newest Chunk that "to" falls in.  If "to" extends to after the newest data
	// then just return the newest chunk.
	// some examples to clarify this more. assume newestChunk.T0 is at 120, then
	// for a to of 121 -> data up to (incl) 120 -> stay at this chunk, it has a point we need
	// for a to of 120 -> data up to (incl) 119 -> use older chunk
	// for a to of 119 -> data up to (incl) 118 -> use older chunk
	newestPos := a.currentChunkPos
	for to <= newestChunk.Series.T0 {
		newestPos--
		if newestPos < 0 {
			newestPos += len(a.chunks)
		}
		newestChunk = a.chunks[newestPos]
	}

	// now just start at oldestPos and move through the Chunks circular Buffer to newestPos
	for {
		c := a.chunks[oldestPos]
		result.Iters = append(result.Iters, c.Series.Iter())

		if oldestPos == newestPos {
			break
		}

		oldestPos++
		if oldestPos >= len(a.chunks) {
			oldestPos = 0
		}
	}

	if oldestChunk.First {
		result.Oldest = a.firstTs
	} else {
		result.Oldest = oldestChunk.Series.T0
	}

	memToIterDuration.Value(time.Now().Sub(pre))
	return result, nil
}

// caller must hold lock
func (a *AggMetric) addAggregators(ts uint32, val float64) {
	for _, agg := range a.aggregators {
		if log.IsLevelEnabled(log.DebugLevel) {
			log.Debugf("AM: %s pushing %d,%f to aggregator %d", a.key, ts, val, agg.span)
		}
		agg.Add(ts, val)
	}
}

// pushToCache adds the chunk into the cache if it is hot
// caller must hold lock
func (a *AggMetric) pushToCache(c *chunk.Chunk) {
	// Only pay the cost of encode if this metric is cacheable
	if !a.flusher.IsCacheable(a.key) {
		return
	}
	intervalHint := a.key.Archive.Span()

	itergen, err := chunk.NewIterGen(c.Series.T0, intervalHint, c.Encode(a.chunkSpan))
	if err != nil {
		log.Errorf("AM: %s failed to generate IterGen. this should never happen: %s", a.key, err)
	}
	go a.flusher.Cache(a.key, 0, itergen)
}

// write a chunk to persistent storage.
// never persist a chunk that may receive further updates!
// (because the stores will read out chunk data on the unlocked chunk)
// caller must hold lock.
func (a *AggMetric) persist(pos int) {
	chunk := a.chunks[pos]
	pre := time.Now()

	lastSaveStart := atomic.LoadUint32(&a.lastSaveStart)
	if lastSaveStart >= chunk.Series.T0 {
		// this can happen if
		// a) there are 2 primary MT nodes both saving chunks to Cassandra
		// b) a primary failed and this node was promoted to be primary but metric consuming is lagging.
		// c) chunk was persisted by GC (stale) and then new data triggered another persist call
		// d) dropFirstChunk is enabled and this is the first chunk
		log.Debugf("AM: persist(): duplicate persist call for chunk.")
		return
	}

	// create an array of chunks that need to be sent to the writeQueue.
	pending := make([]*ChunkWriteRequest, 1)
	// add the current chunk to the list of chunks to send to the writeQueue
	cwr := NewChunkWriteRequest(
		a.SyncChunkSaveState(chunk.Series.T0, true),
		a.key,
		a.ttl,
		chunk.Series.T0,
		chunk.Encode(a.chunkSpan),
		time.Now(),
	)
	pending[0] = &cwr

	// if we recently became the primary, there may be older chunks
	// that the old primary did not save.  We should check for those
	// and save them.
	previousPos := pos - 1
	if previousPos < 0 {
		previousPos += len(a.chunks)
	}
	previousChunk := a.chunks[previousPos]
	for (previousChunk.Series.T0 < chunk.Series.T0) && (lastSaveStart < previousChunk.Series.T0) {
		log.Debugf("AM: persist(): old chunk needs saving. Adding %s:%d to writeQueue", a.key, previousChunk.Series.T0)
		cwr := NewChunkWriteRequest(
			a.SyncChunkSaveState(previousChunk.Series.T0, true),
			a.key,
			a.ttl,
			previousChunk.Series.T0,
			previousChunk.Encode(a.chunkSpan),
			time.Now(),
		)
		pending = append(pending, &cwr)
		previousPos--
		if previousPos < 0 {
			previousPos += len(a.chunks)
		}
		previousChunk = a.chunks[previousPos]
	}

	// Every chunk with a T0 <= this chunks' T0 is now either saved, or in the writeQueue.
	util.AtomicBumpUint32(&a.lastSaveStart, chunk.Series.T0)

	log.Debugf("AM: persist(): sending %d chunks to write queue", len(pending))

	pendingChunk := len(pending) - 1

	// if the store blocks,
	// the calling function will block waiting for persist() to complete.
	// This is intended to put backpressure on our message handlers so
	// that they stop consuming messages, leaving them to buffer at
	// the message bus. The "pending" array of chunks are processed
	// last-to-first ensuring that older data is added to the store
	// before newer data.
	for pendingChunk >= 0 {
		log.Debugf("AM: persist(): sealing chunk %d/%d (%s:%d) and adding to write queue.", pendingChunk, len(pending), a.key, chunk.Series.T0)
		a.flusher.Store(pending[pendingChunk])
		pendingChunk--
	}
	persistDuration.Value(time.Now().Sub(pre))
}

// don't ever call with a ts of 0, cause we use 0 to mean not initialized!
func (a *AggMetric) Add(ts uint32, val float64) {
	if ts < a.ingestFromT0 {
		// TODO: add metric to keep track of the # of points discarded
		if log.IsLevelEnabled(log.DebugLevel) {
			log.Debugf("AM: discarding metric <%d,%f>: does not belong to a chunk starting after ingest-from. First chunk considered starts at %d", ts, val, a.ingestFromT0)
		}
		// even if a point is too old for our raw data, it may not be too old for aggregated data
		// for example let's say a chunk starts at t0=3600 but we have 300-secondly aggregates
		// that mean the aggregators need data from 3301 and onwards, because we aggregate 3301-3600 into a point with ts=3600
		a.Lock()
		a.addAggregators(ts, val)
		a.Unlock()
		return
	}

	// need to check if ts > futureTolerance to prevent that we reject a datapoint
	// because the ts value has wrapped around the uint32 boundary
	if ts > a.futureTolerance && int64(ts-a.futureTolerance) > time.Now().Unix() {
		sampleTooFarAhead.Inc()

		if enforceFutureTolerance {
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("AM: discarding metric <%d,%f>: timestamp is too far in the future, accepting timestamps up to %d seconds into the future", ts, val, a.futureTolerance)
			}

			discardedSampleTooFarAhead.Inc()
			PromDiscardedSamples.WithLabelValues(tooFarAhead, strconv.Itoa(int(a.key.MKey.Org))).Inc()
			return
		}
	}

	a.Lock()
	defer a.Unlock()

	if a.rob == nil {
		// write directly
		a.add(ts, val)
	} else {
		// write through reorder buffer
		pt, res, err := a.rob.Add(ts, val)

		if err == nil {
			if pt.Ts == 0 {
				a.lastWrite = uint32(time.Now().Unix())
			} else {
				a.add(pt.Ts, pt.Val)
				for _, p := range res {
					a.add(p.Ts, p.Val)
				}
			}
		} else {
			log.Debugf("AM: failed to add metric to reorder buffer for %s. %s", a.key, err)
			a.discardedMetricsInc(err)
		}
	}
}

// don't ever call with a ts of 0, cause we use 0 to mean not initialized!
// caller must hold write lock
func (a *AggMetric) add(ts uint32, val float64) {
	t0 := ts - (ts % a.chunkSpan)

	if len(a.chunks) == 0 {
		chunkCreate.Inc()
		// no data has been added to this AggMetric yet.
		// note that we may not be aware of prior data that belongs into this chunk
		// so we should track this cutoff point
		a.chunks = append(a.chunks, chunk.NewFirst(t0))
		a.firstTs = ts

		if err := a.chunks[0].Push(ts, val); err != nil {
			panic(fmt.Sprintf("FATAL ERROR: this should never happen. Pushing initial value <%d,%f> to new chunk at pos 0 failed: %q", ts, val, err))
		}
		totalPoints.Inc()

		log.Debugf("AM: %s Add(): created first chunk with first point: %v", a.key, a.chunks[0])
		a.lastWrite = uint32(time.Now().Unix())
		if a.dropFirstChunk {
			util.AtomicBumpUint32(&a.lastSaveStart, t0)
		}
		a.addAggregators(ts, val)
		return
	}

	currentChunk := a.chunks[a.currentChunkPos]

	if t0 == currentChunk.Series.T0 {
		// last prior data was in same chunk as new point
		if currentChunk.Series.Finished {
			// if we've already 'finished' the chunk, it means it has the end-of-stream marker and any new points behind it wouldn't be read by an iterator
			// you should monitor this metric closely, it indicates that maybe your GC settings don't match how you actually send data (too late)
			discardedReceivedTooLate.Inc()
			PromDiscardedSamples.WithLabelValues(receivedTooLate, strconv.Itoa(int(a.key.MKey.Org))).Inc()
			return
		}

		if err := currentChunk.Push(ts, val); err != nil {
			log.Debugf("AM: failed to add metric to chunk for %s. %s", a.key, err)
			a.discardedMetricsInc(err)
			return
		}
		totalPoints.Inc()
		a.lastWrite = uint32(time.Now().Unix())

		if log.IsLevelEnabled(log.DebugLevel) {
			log.Debugf("AM: %s Add(): pushed new value to last chunk: %v", a.key, a.chunks[0])
		}
	} else if t0 < currentChunk.Series.T0 {
		log.Debugf("AM: Point at %d has t0 %d, goes back into previous chunk. CurrentChunk t0: %d, LastTs: %d", ts, t0, currentChunk.Series.T0, currentChunk.Series.T)
		discardedSampleOutOfOrder.Inc()
		PromDiscardedSamples.WithLabelValues(sampleOutOfOrder, strconv.Itoa(int(a.key.MKey.Org))).Inc()
		return
	} else {
		// Data belongs in a new chunk.

		// If it isn't finished already, add the end-of-stream marker and flag the chunk as "closed"
		currentChunk.Finish()

		a.pushToCache(currentChunk)
		// If we are a primary node, then add the chunk to the write queue to be saved to Cassandra
		if cluster.Manager.IsPrimary() {
			log.Debugf("AM: persist(): node is primary, saving chunk. %s T0: %d", a.key, currentChunk.Series.T0)
			// persist the chunk. If the writeQueue is full, then this will block.
			a.persist(a.currentChunkPos)
		}

		a.currentChunkPos++
		if a.currentChunkPos >= cap(a.chunks) {
			a.currentChunkPos = 0
		}

		chunkCreate.Inc()
		if len(a.chunks) < cap(a.chunks) {
			a.chunks = append(a.chunks, chunk.New(t0))
			if err := a.chunks[a.currentChunkPos].Push(ts, val); err != nil {
				panic(fmt.Sprintf("FATAL ERROR: this should never happen. Pushing initial value <%d,%f> to new chunk at pos %d failed: %q", ts, val, a.currentChunkPos, err))
			}
			totalPoints.Inc()
			log.Debugf("AM: %s Add(): added new chunk to buffer. now %d chunks. and added the new point: %s", a.key, a.currentChunkPos+1, a.chunks[a.currentChunkPos])
		} else {
			chunkClear.Inc()
			totalPoints.DecUint64(uint64(a.chunks[a.currentChunkPos].NumPoints))

			a.chunks[a.currentChunkPos] = chunk.New(t0)
			if err := a.chunks[a.currentChunkPos].Push(ts, val); err != nil {
				panic(fmt.Sprintf("FATAL ERROR: this should never happen. Pushing initial value <%d,%f> to new chunk at pos %d failed: %q", ts, val, a.currentChunkPos, err))
			}
			totalPoints.Inc()
			log.Debugf("AM: %s Add(): cleared chunk at %d of %d and replaced with new. and added the new point: %s", a.key, a.currentChunkPos, len(a.chunks), a.chunks[a.currentChunkPos])
		}
		a.lastWrite = uint32(time.Now().Unix())

	}
	a.addAggregators(ts, val)
}

// collectable returns whether the AggMetric is garbage collectable
// an Aggmetric is collectable based on two conditions:
//   - the AggMetric hasn't been written to in a configurable amount of time
//     (wether the write went to the ROB or a chunk is irrelevant)
//   - the last chunk - if any - is no longer "active".
//     active means:
//     any reasonable realtime stream (e.g. up to 15 min behind wall-clock)
//     could add points to the chunk
//
// caller must hold lock
func (a *AggMetric) collectable(now, chunkMinTs uint32) bool {

	// no chunks at all means "possibly collectable"
	// the caller (AggMetric.GC()) still has its own checks to
	// handle the "no chunks" correctly later.
	// also: we want AggMetric.GC() to go ahead with flushing the ROB in this case
	if len(a.chunks) == 0 {
		return a.lastWrite < chunkMinTs
	}

	currentChunk := a.chunks[a.currentChunkPos]
	return a.lastWrite < chunkMinTs && currentChunk.Series.T0+a.chunkSpan+15*60 < now
}

// GC returns whether or not this AggMetric is stale and can be removed, and its pointcount if so
// chunkMinTs -> min timestamp of a chunk before to be considered stale and to be persisted to Cassandra
// metricMinTs -> min timestamp for a metric before to be considered stale and to be purged from the tank
func (a *AggMetric) GC(now, chunkMinTs, metricMinTs uint32) (uint32, bool) {
	a.Lock()
	defer a.Unlock()

	// unless it looks like the AggMetric is collectable, abort and mark as not stale
	if !a.collectable(now, chunkMinTs) {
		return 0, false
	}

	// make sure any points in the reorderBuffer are moved into our chunks so we can save the data
	if a.rob != nil {
		tmpLastWrite := a.lastWrite
		pts := a.rob.Flush()
		for _, p := range pts {
			a.add(p.Ts, p.Val)
		}

		// adding points will cause our lastWrite to be updated, but we want to keep the old value
		a.lastWrite = tmpLastWrite
	}

	// this aggMetric has never had metrics written to it.
	if len(a.chunks) == 0 {
		return a.gcAggregators(now, chunkMinTs, metricMinTs)
	}

	currentChunk := a.chunks[a.currentChunkPos]

	// we must check collectable again. Imagine this scenario:
	// * we didn't have any chunks when calling collectable() the first time so it returned true
	// * data from the ROB is flushed and moved into a new chunk
	// * this new chunk is active so we're not collectable, even though earlier we thought we were.
	if !a.collectable(now, chunkMinTs) {
		return 0, false
	}

	if !currentChunk.Series.Finished {
		// chunk hasn't been written to in a while, and is not yet closed.
		// Let's close it and persist it if we are a primary
		log.Debugf("AM: Found stale Chunk, adding end-of-stream bytes. key: %v T0: %d", a.key, currentChunk.Series.T0)
		currentChunk.Finish()
		a.pushToCache(currentChunk)
		if cluster.Manager.IsPrimary() {
			log.Debugf("AM: persist(): node is primary, saving chunk. %v T0: %d", a.key, currentChunk.Series.T0)
			// persist the chunk. If the writeQueue is full, then this will block.
			a.persist(a.currentChunkPos)
		}
	}

	var points uint32
	for _, chunk := range a.chunks {
		points += chunk.NumPoints
	}
	p, stale := a.gcAggregators(now, chunkMinTs, metricMinTs)
	points += p
	return points, stale && a.lastWrite < metricMinTs
}

// gcAggregators returns whether all aggregators are stale and can be removed, and their pointcount if so
// caller should hold write lock
func (a *AggMetric) gcAggregators(now, chunkMinTs, metricMinTs uint32) (uint32, bool) {
	var points uint32
	stale := true
	for _, agg := range a.aggregators {
		p, s := agg.GC(now, chunkMinTs, metricMinTs, a.lastWrite)
		points += p
		stale = stale && s
	}
	return points, stale
}

func (a *AggMetric) discardedMetricsInc(err error) {
	var reason string
	switch err {
	case mdataerrors.ErrMetricTooOld:
		reason = sampleOutOfOrder
		discardedSampleOutOfOrder.Inc()
	case mdataerrors.ErrMetricNewValueForTimestamp:
		reason = newValueForTimestamp
		discardedNewValueForTimestamp.Inc()
	default:
		discardedUnknown.Inc()
		reason = "unknown"
	}
	PromDiscardedSamples.WithLabelValues(reason, strconv.Itoa(int(a.key.MKey.Org))).Inc()
}
