package kafkamdm

import (
	"sync"
)

type lagLogger struct {
	sync.Mutex
	pos          int
	measurements []int
}

func newLagLogger(size int) *lagLogger {
	return &lagLogger{
		pos:          0,
		measurements: make([]int, 0, size),
	}
}

func (l *lagLogger) Store(lag int) {
	l.Lock()
	defer l.Unlock()
	l.pos++
	if len(l.measurements) < cap(l.measurements) {
		l.measurements = append(l.measurements, lag)
		return
	}

	if l.pos >= cap(l.measurements) {
		l.pos = 0
	}
	l.measurements[l.pos] = lag
}

func (l *lagLogger) Min() int {
	l.Lock()
	defer l.Unlock()
	min := -1
	for _, m := range l.measurements {
		if min < 0 || m < min {
			min = m
		}
	}
	if min < 0 {
		min = 0
	}
	return min
}

/*
   LagMonitor is used to determine how upToDate this node is.
   We periodically collect the lag for each partition, keeping the last N
   measurements in a moving window.  Using those measurements we can then
   compute a overall score for this node. The score is just the maximum minimum
   lag of all partitions.

   For each partition we get the minimum lag seen in the last N measurements.
   Using minimum ensures that transient issues dont affect the health score.
   From each of those per-partition values, we then get the maximum.  This
   ensures that overall health is based on the worst performing partition.
*/
type LagMonitor struct {
	lag map[int32]*lagLogger
}

func NewLagMonitor(size int, partitions []int32) *LagMonitor {
	m := &LagMonitor{
		lag: make(map[int32]*lagLogger),
	}
	for _, p := range partitions {
		m.lag[p] = newLagLogger(size)
	}
	return m
}

func (l *LagMonitor) Metric() int {
	max := 0
	for _, lag := range l.lag {
		val := lag.Min()
		if val > max {
			max = val
		}
	}
	return max
}

func (l *LagMonitor) Store(partition int32, val int) {
	l.lag[partition].Store(val)
}
