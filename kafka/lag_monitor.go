package kafka

import (
	"sync"
	"time"
)

// lagLogger maintains a set of most recent lag measurements
// and is able to provide the lowest value seen.
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

// Store saves the current value, potentially overwriting an old value
// if needed.
// Note: negative values are ignored.  We rely on previous data - if any - in such case.
// negative values can happen when:
//  - kafka had to recover, and a previous offset loaded from offsetMgr was bigger than current offset
//  - a rollover of the offset counter
func (l *lagLogger) Store(lag int) {
	if lag < 0 {
		return
	}
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

// Min returns the lowest lag seen (0 or positive) or -1 if no lags reported yet
// note: values may be slightly out of date if negative values were reported
// (see Store())
func (l *lagLogger) Min() int {
	l.Lock()
	defer l.Unlock()
	if len(l.measurements) == 0 {
		return -1
	}
	min := -1
	for _, m := range l.measurements {
		if min < 0 || m < min {
			min = m
		}
	}
	return min
}

type rateLogger struct {
	sync.Mutex
	lastOffset int64
	lastTs     time.Time
	rate       int64
}

func newRateLogger() *rateLogger {
	return &rateLogger{}
}

// Store saves the current offset and updates the rate if it is confident
// offset must be concrete values, not logical values like -2 (oldest) or -1 (newest)
func (o *rateLogger) Store(offset int64, ts time.Time) {
	o.Lock()
	defer o.Unlock()
	if o.lastTs.IsZero() {
		// first measurement
		o.lastOffset = offset
		o.lastTs = ts
		return
	}
	duration := ts.Sub(o.lastTs)
	if duration < time.Second && duration > 0 {
		// too small difference. either due to clock adjustment or this method
		// is called very frequently, e.g. due to a subsecond offset-commit-interval.
		// We need to let more time pass to make an accurate calculation.
		return
	}
	if duration <= 0 {
		// current ts is <= last ts. This would only happen if clock went back in time
		// in which case we can't reliably work out how
		// long it has really been since we last took a measurement.
		// but set a new baseline for next time
		o.lastTs = ts
		o.lastOffset = offset
		return
	}
	metrics := offset - o.lastOffset
	o.lastTs = ts
	o.lastOffset = offset
	if metrics < 0 {
		// this is possible if our offset counter rolls over or is reset.
		// If it was a rollover we could compute the rate, but it is safer
		// to just keep using the last computed rate, and wait for the next
		// measurement to compute a new rate based on the new baseline
		return
	}
	// note the multiplication overflows if you have 9 billion metrics
	// sice max int64 is 9 223 372 036 854 775 807 (not an issue in practice)
	o.rate = (1e9 * metrics / int64(duration)) // metrics/ns -> metrics/s
	return
}

// Rate returns the last reliable rate calculation
// * generally, it's the last reported measurement
// * occasionally, it's one report interval in the past (due to rollover)
// * exceptionally, it's an old measurement (if you keep adjusting the system clock)
// after startup, reported rate may be 0 if we haven't been up long enough to determine it yet.
func (o *rateLogger) Rate() int64 {
	o.Lock()
	defer o.Unlock()
	return o.rate
}

// LagMonitor determines how upToDate this node is.
// For each partition, we periodically collect:
// * the consumption lag (we keep the last N measurements)
// * ingest rate
// We then combine this data into a score, see the Metric() method.
type LagMonitor struct {
	lag  map[int32]*lagLogger
	rate map[int32]*rateLogger
}

func NewLagMonitor(size int, partitions []int32) *LagMonitor {
	m := &LagMonitor{
		lag:  make(map[int32]*lagLogger),
		rate: make(map[int32]*rateLogger),
	}
	for _, p := range partitions {
		m.lag[p] = newLagLogger(size)
		m.rate[p] = newRateLogger()
	}
	return m
}

// Metric computes the overall score of up-to-date-ness of this node,
// as an estimated number of seconds behind kafka.
// We first compute the score for each partition like so:
// (minimum lag seen in last N measurements) / ingest rate.
// example:
// lag (in messages/metrics)     ingest rate       --->    score (seconds behind)
//                       10k       1k/second                 10
//                       200       1k/second                  0 (less than 1s behind)
//                         0               *                  0 (perfectly in sync)
//                   anything     0 (after startup)          same as lag
//
// The returned total score for the node is the max of the scores of individual partitions.
// Note that one or more StoreOffset() (rate) calls may have been made but no StoreLag().
// This can happen in 3 cases:
// - we're not consuming yet
// - trouble querying the partition for latest offset
// - consumePartition() has called StoreOffset() but the code hasn't advanced yet to StoreLag()
func (l *LagMonitor) Metric() int {
	max := 0
	for p, lag := range l.lag {
		rate := l.rate[p]
		l := lag.Min()   // accurate lag, -1 if unknown
		r := rate.Rate() // accurate rate, or 0 if we're not sure.
		if r == 0 {
			r = 1
		}
		var val int
		if l == -1 {
			// if we have no lag measurements yet,
			// just assign a priority of 10k for this partition
			val = 10000
		} else {
			val = l / int(r)
		}
		if val > max {
			max = val
		}
	}
	return max
}

func (l *LagMonitor) StoreLag(partition int32, val int) {
	l.lag[partition].Store(val)
}

func (l *LagMonitor) StoreOffset(partition int32, offset int64, ts time.Time) {
	l.rate[partition].Store(offset, ts)
}
