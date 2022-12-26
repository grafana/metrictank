package kafkamdm

import (
	"math"
	"sync"
	"time"
)

type offsetMeasurement struct {
	readOffset    int64 // last offset consumed
	highWaterMark int64 // last offset in the topic
	ts            time.Time
}

// lagLogger maintains a set of most recent lag measurements
// and is able to provide the lowest value seen.
type lagLogger struct {
	pos          int
	measurements []offsetMeasurement
}

func newLagLogger(size int) *lagLogger {
	return &lagLogger{
		pos:          0,
		measurements: make([]offsetMeasurement, 0, size),
	}
}

// Store saves the current offsets and timestamp, potentially overwriting an old value
// if needed.
// Note: negative values of highWaterMark - readOffset are ignored.  We rely on previous data - if any - in such case.
// negative values can happen upon a rollover of the offset counter
func (l *lagLogger) Store(readOffset, highWaterMark int64, ts time.Time) {
	lag := highWaterMark - readOffset
	if lag < 0 {
		return
	}

	measurement := offsetMeasurement{
		readOffset:    readOffset,
		highWaterMark: highWaterMark,
		ts:            ts,
	}
	if len(l.measurements) < cap(l.measurements) {
		l.measurements = append(l.measurements, measurement)
		l.pos = len(l.measurements) - 1
		return
	}

	l.pos++

	if l.pos >= cap(l.measurements) {
		l.pos = 0
	}
	l.measurements[l.pos] = measurement
}

// Min returns the lowest lag seen (0 or positive) or -1 if no lags reported yet
// note: values may be slightly out of date if negative values were reported
// (see Store())
func (l *lagLogger) Min() int {
	if len(l.measurements) == 0 {
		return -1
	}
	min := int(l.measurements[0].highWaterMark - l.measurements[0].readOffset)
	for _, m := range l.measurements[1:] {
		lag := int(m.highWaterMark - m.readOffset)
		if lag < min {
			min = lag
		}
	}
	return min
}

// Rate returns an average rate calculation over the last N measurements (configured at construction)
// after startup, reported rate may be 0 if we haven't been up long enough to determine it yet.
func (l *lagLogger) Rate() int64 {
	if len(l.measurements) < 2 {
		return 0
	}

	latestLag := l.measurements[l.pos]
	var earliestLag offsetMeasurement

	if len(l.measurements) == cap(l.measurements) {
		earliestLag = l.measurements[(l.pos+1)%len(l.measurements)]
	} else {
		earliestLag = l.measurements[0]
	}

	high, low := latestLag.highWaterMark, earliestLag.highWaterMark

	// If there are no longer any incoming messages, use our read offsets to compute rate
	if high == low {
		high, low = latestLag.readOffset, earliestLag.readOffset
	}
	if high < low {
		// Offset must have wrapped around...seems unlikely. Estimate growth using MaxInt64
		high = math.MaxInt64
	}

	totalGrowth := float64(high - low)
	totalDurationSec := float64(latestLag.ts.UnixNano()-earliestLag.ts.UnixNano()) / float64(time.Second.Nanoseconds())

	return int64(totalGrowth / totalDurationSec)
}

// LagMonitor determines how upToDate this node is.
// For each partition, we periodically collect:
// * the consumption lag (we keep the last N measurements)
// * ingest rate
// We then combine this data into a score, see the Metric() method.
type LagMonitor struct {
	sync.Mutex
	monitors    map[int32]*lagLogger
	explanation Explanation
}

type Explanation struct {
	Status   map[int32]Status
	Priority int
	Updated  time.Time
}

type Status struct {
	Lag      int
	Rate     int
	Priority int
}

func NewLagMonitor(size int, partitions []int32) *LagMonitor {
	m := &LagMonitor{
		monitors: make(map[int32]*lagLogger),
	}
	for _, p := range partitions {
		m.monitors[p] = newLagLogger(size)
	}
	return m
}

// Metric computes the overall score of up-to-date-ness of this node,
// as an estimated number of seconds behind kafka.
// We first compute the score for each partition like so:
// (minimum lag seen in last N measurements) / input rate.
// example:
// lag (in messages/metrics)     input rate       --->    score (seconds behind)
//
//	    10k       1k/second                 10
//	    200       1k/second                  0 (less than 1s behind)
//	      0               *                  0 (perfectly in sync)
//	anything     0 (after startup)          same as lag
//
// The returned total score for the node is the max of the scores of individual partitions.
// Note that one or more StoreOffset() (rate) calls may have been made but no StoreLag().
// This can happen in 3 cases:
// - we're not consuming yet
// - trouble querying the partition for latest offset
// - consumePartition() has called StoreOffset() but the code hasn't advanced yet to StoreLag()
func (l *LagMonitor) Metric() int {
	l.Lock()
	defer l.Unlock()
	l.explanation = Explanation{
		Status:  make(map[int32]Status),
		Updated: time.Now(),
	}
	max := 0
	for p, lag := range l.monitors {
		status := l.getPartitionPriority(p, lag)
		if status.Priority > max {
			max = status.Priority
		}
		l.explanation.Status[p] = status
	}
	l.explanation.Updated = time.Now()
	l.explanation.Priority = max
	return max
}

func (l *LagMonitor) getPartitionPriority(partition int32, lag *lagLogger) Status {
	status := Status{
		Lag:  lag.Min(),       // accurate lag, -1 if unknown
		Rate: int(lag.Rate()), // accurate rate, or 0 if we're not sure
	}
	if status.Lag == -1 {
		// if we have no lag measurements yet,
		// just assign a priority of 10k for this partition
		status.Priority = 10000
	} else {
		// if we're not sure of rate, we don't want divide by zero
		// instead assume rate is super low
		if status.Rate == 0 {
			status.Priority = status.Lag
		} else {
			status.Priority = status.Lag / status.Rate
		}
	}

	return status
}

func (l *LagMonitor) GetPartitionPriority(partition int32) int {
	l.Lock()
	defer l.Unlock()

	var lag *lagLogger
	var ok bool
	if lag, ok = l.monitors[partition]; !ok {
		// if we have no lag measurements yet,
		// just assign a priority of 10k for this partition
		return 10000
	}

	return l.getPartitionPriority(partition, lag).Priority
}

func (l *LagMonitor) Explain() interface{} {
	l.Lock()
	defer l.Unlock()
	return struct {
		Explanation Explanation
		Now         time.Time
	}{
		Explanation: l.explanation,
		Now:         time.Now(),
	}
}

func (l *LagMonitor) StoreOffsets(partition int32, readOffset, highWaterMark int64, ts time.Time) {
	l.Lock()
	l.monitors[partition].Store(readOffset, highWaterMark, ts)
	l.Unlock()
}
