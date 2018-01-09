package hist15s

import (
	"math"
	"sync/atomic"
	"time"
)

const maxVal = uint32(29999999) // used to report max number as 29s even if it's higher

// Hist15s is optimized for measurements between 1ms and 15s
type Hist15s struct {
	limits [32]uint32 // in micros
	counts [32]uint32
}

func New() Hist15s {
	return Hist15s{
		limits: [32]uint32{
			1000,           // 0
			2000,           // 1
			3000,           // 2
			5000,           // 3
			7500,           // 4
			10000,          // 5
			15000,          // 6
			20000,          // 7
			30000,          // 8
			40000,          // 9
			50000,          // 10
			65000,          // 11
			80000,          // 12
			100000,         // 13
			150000,         // 14
			200000,         // 15
			300000,         // 16
			400000,         // 17
			500000,         // 18
			650000,         // 19
			800000,         // 20
			1000000,        // 21
			1500000,        // 22
			2000000,        // 23
			3000000,        // 24
			4000000,        // 25
			5000000,        // 26
			6500000,        // 27
			8000000,        // 28
			10000000,       // 29
			15000000,       // 30
			math.MaxUint32, // 31 // to ease binary search, but will be reported as 29s
		},
	}
}

// searchBucket implements a binary search, to find the bucket i to insert val in, like so:
// limits[i-1] < val <= limits[i]
// if we can convince the go compiler to inline this we can get a 14~22% speedup (verified by manually patching it in)
// but we can't :( see https://github.com/golang/go/issues/17566
// so for now, we just replicate this code in addDuration below. make sure to keep the code in sync!
func searchBucket(limits [32]uint32, micros uint32) int {
	min, i, max := 0, 16, 32
	for {
		if micros <= limits[i] {
			if i == 0 || micros > limits[i-1] {
				return i
			}
			max = i
		} else {
			min = i
		}
		i = min + ((max - min) / 2)
	}
}

// adds to the right bucket with a copy of the searchBucket function below, to enforce inlining.
func (h *Hist15s) AddDuration(value time.Duration) {
	// note: overflows at 4294s, but if you have values this high,
	// you are definitely not using this histogram for the target use case.
	micros := uint32(value.Nanoseconds() / 1000)
	min, i, max := 0, 16, 32
	for {
		if micros <= h.limits[i] {
			if i == 0 || micros > h.limits[i-1] {
				atomic.AddUint32(&h.counts[i], 1)
				return
			}
			max = i
		} else {
			min = i
		}
		i = min + ((max - min) / 2)
	}
}

// Snapshot returns a snapshot of the data and resets internal state
func (h *Hist15s) Snapshot() []uint32 {
	snap := make([]uint32, 32)
	for i := 0; i < 32; i++ {
		snap[i] = atomic.SwapUint32(&h.counts[i], 0)
	}
	return snap
}

// if count is 0 then there was no input, so statistical summaries are invalid
type Report struct {
	Min    uint32 // in micros
	Mean   uint32 // in micros
	Median uint32 // in micros
	P75    uint32 // in micros
	P90    uint32 // in micros
	Max    uint32 // in micros
	Count  uint32
}

// ok can be false for two reasons:
// (in either case, we can't compute the summaries)
// * the total count was 0
// * the total count overflowed (but we set to 1 so you can tell the difference)
func (h *Hist15s) Report(data []uint32) (Report, bool) {
	totalValue := uint64(0)
	r := Report{}
	for i, count := range data {
		if count > 0 {
			remainder := math.MaxUint32 - r.Count
			if count > remainder {
				r.Count = 1
				return r, false
			}
			limit := h.limits[i]
			// special case, report as 29s
			if i == 31 {
				limit = maxVal
			}
			if r.Min == 0 { // this means we haven't found min yet.
				r.Min = limit
			}
			r.Max = limit
			r.Count += count
			totalValue += uint64(count * limit)
		}
	}
	if r.Count == 0 {
		return r, false
	}
	r.Median = h.limits[Quantile(data, 0.50, r.Count)]
	if r.Median == math.MaxUint32 {
		r.Median = maxVal
	}
	r.P75 = h.limits[Quantile(data, 0.75, r.Count)]
	if r.P75 == math.MaxUint32 {
		r.P75 = maxVal
	}
	r.P90 = h.limits[Quantile(data, 0.90, r.Count)]
	if r.P90 == math.MaxUint32 {
		r.P90 = maxVal
	}
	r.Mean = uint32(totalValue / uint64(r.Count))
	return r, true
}

// quantile q means what's the value v so that all q of the values have value <= v
// TODO since we typically get 99%, 99.9%, 75%, etc. it should be faster to count the other way
func Quantile(data []uint32, q float64, total uint32) int {
	count := q * float64(total)
	for i := 0; i < 32; i++ {
		count -= float64(data[i])

		if count <= 0 {
			return i // we return the upper limit, real quantile would be less, but we can't make the result any better.
		}
	}

	return -1
}
