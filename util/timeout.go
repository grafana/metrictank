package util

import (
	"time"
)

// TimeBoundWithCacheFunc decorates a function that has a return value in order to bound its execution time.
// When the decorated function is called, if the original function takes more than 'timeout' to execute,
// the returned value will be the value returned by a previous call. If no previous call was performed,
// the call will block until the original function returns.
func TimeBoundWithCacheFunc(fn func() interface{}, timeout, maxAge time.Duration) func() interface{} {
	var previousResult interface{}
	var previousTimestamp time.Time

	return func() interface{} {
		done := make(chan interface{}, 1)
		timer := time.NewTimer(timeout)

		go func() {
			result := fn()
			done <- result
			close(done)
		}()

		var result interface{}

		select {
		case result = <-done:
			// call succeeded in time, use its result
			// avoid timers from staying alive
			// see https://medium.com/@oboturov/golang-time-after-is-not-garbage-collected-4cbc94740082
			timer.Stop()
		case <-timer.C:
			// call took too long, use cached result if not too old
			if time.Since(previousTimestamp) < maxAge && previousResult != nil {
				return previousResult
			} else {
				// in case the result did not arrive in time but the previous result was too old
				result = <-done
			}
		}

		previousTimestamp = time.Now()
		previousResult = result
		return result
	}
}
