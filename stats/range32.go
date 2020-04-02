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
	name  []byte
	tags  []byte
}

func NewRange32(name string) *Range32 {
	return NewRange32WithTags(name, "")
}

func NewRange32WithTags(name, tags string) *Range32 {
	return registry.getOrAdd(name+tags, &Range32{
		min:  math.MaxUint32,
		name: []byte(name),
		tags: []byte(tags),
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

func (r *Range32) WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte {
	r.Lock()
	// if no values were seen, don't report anything to Graphite
	if r.valid {
		buf = WriteUint32(buf, prefix, r.name, []byte(".min.gauge32"), r.tags, r.min, now)
		buf = WriteUint32(buf, prefix, r.name, []byte(".max.gauge32"), r.tags, r.max, now)
		r.min = math.MaxUint32
		r.max = 0
		r.valid = false
	}
	r.Unlock()
	return buf
}
