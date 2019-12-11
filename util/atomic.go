package util

import (
	"sync/atomic"
)

// AtomicallySwapIfLarger<Type> increases the value at the given location to the given value, unless the given value is not higher
// than the present value. In case of a race condition where multiple routines are executing this function on one location concurrently,
// the final value once they have all returned will be the highest value passed into any of them.
// note:
// * someone else may have just concurrently updated value at location to a higher value than what we have, which we should restore
// * by the time we look at the previous value and try to restore it, someone else may have updated it to a higher value
// both these scenarios are unlikely but we should accommodate them anyway.

func AtomicallySwapIfLargerInt64(loc *int64, newVal int64) {
	prev := atomic.SwapInt64(loc, newVal)
	for prev > newVal {
		newVal = prev
		prev = atomic.SwapInt64(loc, newVal)
	}
}

func AtomicallySwapIfLargerUint32(loc *uint32, newVal uint32) {
	prev := atomic.SwapUint32(loc, newVal)
	for prev > newVal {
		newVal = prev
		prev = atomic.SwapUint32(loc, newVal)
	}
}
