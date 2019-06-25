package runner

import (
	"log"
	"sync/atomic"
	"time"
)

type runStats struct {
	addsCompleted    uint32          // how many adds have been executed
	updatesCompleted uint32          // how many updates have been executed
	queriesStarted   uint32          // how many queries have been started
	queriesCompleted uint32          // how many queries have been completed
	queryTimes       []time.Duration // slice of query execution durations in ms
}

func newRunStats(queryCount uint32) runStats {
	log.Printf("Preallocating space for %d queries in stats container", queryCount)
	return runStats{
		queryTimes: make([]time.Duration, queryCount),
	}
}

func (r *runStats) Print(runSeconds uint32) {
	log.Printf("Adds Completed: %d (%f / sec)", r.addsCompleted, float32(r.addsCompleted)/float32(runSeconds))
	log.Printf("Updates Completed: %d (%f / sec)", r.updatesCompleted, float32(r.updatesCompleted)/float32(runSeconds))
	log.Printf("Queries Started: %d (%f / sec)", r.queriesStarted, float32(r.queriesStarted)/float32(runSeconds))
	log.Printf("Queries Completed: %d (%f / sec)", r.queriesCompleted, float32(r.queriesCompleted)/float32(runSeconds))

	var queryTimeSum uint64
	queriesCompleted := atomic.LoadUint32(&r.queriesCompleted)
	for i := uint32(0); i < queriesCompleted; i++ {
		queryTimeSum += uint64(r.queryTimes[i].Nanoseconds())
	}

	log.Printf("Average query time: %d ms", queryTimeSum/uint64(queriesCompleted)/1000000)
}

func (r *runStats) incAddsCompleted() uint32 {
	return atomic.AddUint32(&r.addsCompleted, 1)
}
func (r *runStats) incUpdatesCompleted(delta uint32) uint32 {
	return atomic.AddUint32(&r.updatesCompleted, delta)
}

// incQueriesStarted increases the counter of queries that have been started
// and returns the current value after the update
func (r *runStats) incQueriesStarted() uint32 {
	return atomic.AddUint32(&r.queriesStarted, 1)
}

// incQueriesCompleted increases the counter of queries that have been completed
// it returns the last value of the counter before increasing it
func (r *runStats) incQueriesCompleted() uint32 {
	return atomic.AddUint32(&r.queriesCompleted, 1) - 1
}

// addQueryTime registers the time it took for a query to complete, it assumes
// that the slice r.queryTimes has been preallocated and does not need to
// be grown
func (r *runStats) addQueryTime(queryID uint32, time time.Duration) {
	if queryID >= uint32(len(r.queryTimes)) {
		log.Fatalf("Exhausted slice to collect query times at queryID %d", queryID)
	}
	r.queryTimes[queryID] = time
}
