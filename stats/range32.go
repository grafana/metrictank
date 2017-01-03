package stats

import (
	"math"
	"sync"
	"time"
)

// Range32 computes the min and max of sets of numbers, as 32bit numbers
// example application: queue depths
// min lets you see if the queue is able to drain
// max lets you see how large the queue tends to grow
// concurrency-safe
type Range32 struct {
	sync.Mutex
	min   uint32
	max   uint32
	valid bool // whether any values have been seen
}

func NewRange32(name string) *Range32 {
	return registry.getOrAdd(name, &Range32{
		min: math.MaxUint32,
	},
	).(*Range32)
}

func (r *Range32) Value(val int) {
	r.ValueUint32(uint32(val))
}

func (r *Range32) ValueUint32(val uint32) {
	r.Lock()
	if val < r.min {
		r.min = val
	}
	if val > r.max {
		r.max = val
	}
	r.valid = true
	r.Unlock()
}

func (r *Range32) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	r.Lock()
	// if no values were seen, don't report anything to graphite
	if r.valid {
		buf = WriteUint32(buf, prefix, []byte("min.gauge32"), r.min, now)
		buf = WriteUint32(buf, prefix, []byte("max.gauge32"), r.max, now)
		r.min = math.MaxUint32
		r.max = 0
		r.valid = false
	}
	r.Unlock()
	return buf
}
