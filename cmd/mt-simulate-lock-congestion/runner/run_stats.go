package runner

import (
	"log"
	"sync/atomic"
	"time"
)

type runStats struct {
	addsCompleted    uint32          // how many adds have been executed
	queriesCompleted uint32          // how many queries have been executed
	queryTimes       []time.Duration // slice of query execution durations in ms
	queryTimeCursor  uint32          // tracking position of the last write to queryTimes
}

func newRunStats(queryCount uint32) runStats {
	return runStats{
		queryTimes: make([]time.Duration, queryCount),
	}
}

func (r *runStats) Print(runSeconds uint32) {
	log.Printf("Adds Completed: %d (%f / sec)", r.addsCompleted, float32(r.addsCompleted)/float32(runSeconds))
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

func (r *runStats) incQueriesCompleted() uint32 {
	return atomic.AddUint32(&r.queriesCompleted, 1)
}

func (r *runStats) addQueryTime(time time.Duration) {
	cursor := atomic.AddUint32(&r.queryTimeCursor, 1)
	if cursor >= uint32(len(r.queryTimes)) {
		log.Fatal("Exhausted slice to collect query times")
	}
	r.queryTimes[cursor] = time
}
