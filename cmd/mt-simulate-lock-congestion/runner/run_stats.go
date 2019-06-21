package runner

import (
	"fmt"
	"sync/atomic"
)

type runStats struct {
	addsCompleted    uint32   // how many adds have been executed
	queriesCompleted uint32   // how many queries have been executed
	queryTimes       []uint32 // slice of query execution durations in ms
	queryTimeCursor  uint32   // tracking position of the last write to queryTimes
}

func (r *runStats) Print(runSeconds uint32) {
	fmt.Printf("Adds Completed: %d (%d / sec)", r.addsCompleted, r.addsCompleted/runSeconds)
	fmt.Printf("Queries Completed: %d (%d / sec)", r.queriesCompleted, r.queriesCompleted/runSeconds)

	var queryTimeSum uint64
	for _, queryTime := range r.queryTimes {
		queryTimeSum += uint64(queryTime)
	}
	fmt.Printf("Average query time: %d ns", queryTimeSum/uint64(r.queryTimeCursor))
}

func (r *runStats) incAddsCompleted() uint32 {
	return atomic.AddUint32(&r.addsCompleted, 1)
}

func (r *runStats) incQueriesCompleted() uint32 {
	return atomic.AddUint32(&r.queriesCompleted, 1)
}

func (r *runStats) addQueryTime(time uint32) {
	cursor := atomic.AddUint32(&r.queryTimeCursor, 1)
	if cursor < uint32(len(r.queryTimes)) {
		r.queryTimes[cursor] = time
	}
}
