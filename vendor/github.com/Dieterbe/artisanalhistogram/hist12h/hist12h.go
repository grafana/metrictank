package hist12h

import (
	"math"
	"sync/atomic"
	"time"
)

const maxVal = uint32(86400) // used to report max number as 1day even if it's higher

// Hist12h is optimized for measurements between 500ms and 12h
type Hist12h struct {
	limits [32]uint32 // in millis
	counts [32]uint32
}

func New() Hist12h {
	return Hist12h{
		limits: [32]uint32{
			500,            // 0
			1000,           // 1
			2000,           // 2
			3000,           // 3
			5000,           // 4
			7500,           // 5
			10000,          // 6
			15000,          // 7
			20000,          // 8
			30000,          // 9
			45000,          // 10
			60000,          // 11 // 1min
			90000,          // 12 // 1.5min
			120000,         // 13 // 2min
			180000,         // 14 // 3min
			240000,         // 15 // 4min
			300000,         // 16 // 5min
			450000,         // 17 // 7.5min
			600000,         // 18 // 10min
			750000,         // 19 // 12.5min
			9000000,        // 20 // 15min
			1200000,        // 21 // 20min
			1800000,        // 22 // 30min
			2700000,        // 23 // 45min
			3600000,        // 24 // 1hr
			7200000,        // 25 // 2hr
			10800000,       // 26 // 3h
			16200000,       // 27 // 4.5
			21600000,       // 28 // 6h
			32400000,       // 29 // 9h
			43200000,       // 30 // 12h
			math.MaxUint32, // 31 // to ease binary search, but will be reported as 1day
		},
	}
}

// searchBucket implements a binary search, to find the bucket i to insert val in, like so:
// limits[i-1] < val <= limits[i]
// if we can convince the go compiler to inline this we can get a 14~22% speedup (verified by manually patching it in)
// but we can't :( see https://github.com/golang/go/issues/17566
// so for now, we just replicate this code in addDuration below. make sure to keep the code in sync!
func searchBucket(limits [32]uint32, millis uint32) int {
	min, i, max := 0, 16, 32
	for {
		if millis <= limits[i] {
			if i == 0 || millis > limits[i-1] {
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
func (h *Hist12h) AddDuration(value time.Duration) {
	// note: overflows at 1193 hours, but if you have values this high,
	// you are definitely not using this histogram for the target use case.
	millis := uint32(value.Nanoseconds() / 1000000)
	min, i, max := 0, 16, 32
	for {
		if millis <= h.limits[i] {
			if i == 0 || millis > h.limits[i-1] {
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
func (h *Hist12h) Snapshot() []uint32 {
	snap := make([]uint32, 32)
	for i := 0; i < 32; i++ {
		snap[i] = atomic.SwapUint32(&h.counts[i], 0)
	}
	return snap
}

// if count is 0 then there was no input, so statistical summaries are invalid
type Report struct {
	Min    uint32 // in millis
	Mean   uint32 // in millis
	Median uint32 // in millis
	P75    uint32 // in millis
	P90    uint32 // in millis
	Max    uint32 // in millis
	Count  uint32
}

// ok can be false for two reasons:
// (in either case, we can't compute the summaries)
// * the total count was 0
// * the total count overflowed (but we set to 1 so you can tell the difference)
func (h *Hist12h) Report(data []uint32) (Report, bool) {
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
