package util

import (
	"sync/atomic"
)

// AtomicBumpInt64 assures the given address stores the highest int64 between the current value,
// the value provided and the value provided by any other concurrently executing call
func AtomicBumpInt64(loc *int64, newVal int64) {
	prev := atomic.SwapInt64(loc, newVal)
	for prev > newVal {
		newVal = prev
		prev = atomic.SwapInt64(loc, newVal)
	}
}

// AtomicBumpUint32 assures the given address stores the highest uint32 between the current value,
// the value provided and the value provided by any other concurrently executing call
func AtomicBumpUint32(loc *uint32, newVal uint32) {
	prev := atomic.SwapUint32(loc, newVal)
	for prev > newVal {
		newVal = prev
		prev = atomic.SwapUint32(loc, newVal)
	}
}
