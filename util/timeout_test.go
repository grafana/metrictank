package util

import (
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestTimeBoundWithCacheFunc(t *testing.T) {
	tests := []struct {
		name               string
		functionReturns    []int
		executionDurations []time.Duration
		expectedResults    []int
		timeout            time.Duration
		maxAge             time.Duration
	}{
		{
			name:               "immediate",
			functionReturns:    []int{42},
			executionDurations: []time.Duration{0},
			expectedResults:    []int{42},
			timeout:            50 * time.Millisecond,
			maxAge:             5 * time.Minute,
		},
		{
			name:               "immediate_zero_timeout",
			functionReturns:    []int{42},
			executionDurations: []time.Duration{0},
			expectedResults:    []int{42},
			timeout:            0 * time.Millisecond,
			maxAge:             5 * time.Minute,
		},
		{
			name:               "immediate_zero_maxage",
			functionReturns:    []int{42},
			executionDurations: []time.Duration{0},
			expectedResults:    []int{42},
			timeout:            50 * time.Millisecond,
			maxAge:             0 * time.Minute,
		},
		{
			name:               "slow_notimeout",
			functionReturns:    []int{42},
			executionDurations: []time.Duration{100 * time.Millisecond},
			expectedResults:    []int{42},
			timeout:            500 * time.Millisecond,
			maxAge:             5 * time.Minute,
		},
		{
			name:               "slow_timeout",
			functionReturns:    []int{42},
			executionDurations: []time.Duration{100 * time.Millisecond},
			expectedResults:    []int{42},
			timeout:            50 * time.Millisecond,
			maxAge:             5 * time.Minute,
		},
		{
			name:               "fast_then_slow_timeout",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{10 * time.Millisecond, 100 * time.Millisecond},
			expectedResults:    []int{42, 42},
			timeout:            50 * time.Millisecond,
			maxAge:             5 * time.Minute,
		},
		{
			name:               "fast_then_fast",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond},
			expectedResults:    []int{42, 88},
			timeout:            50 * time.Millisecond,
			maxAge:             5 * time.Minute,
		},
		{
			name:               "slow_timeout_then_slow_timeout",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{100 * time.Millisecond, 100 * time.Millisecond},
			expectedResults:    []int{42, 42},
			timeout:            50 * time.Millisecond,
			maxAge:             5 * time.Minute,
		},
		{
			name:               "fast_then_slow_then_fast_never_timeout",
			functionReturns:    []int{42, 88, 1},
			executionDurations: []time.Duration{1 * time.Millisecond, 100 * time.Millisecond, 2 * time.Millisecond},
			expectedResults:    []int{42, 88, 1},
			timeout:            500 * time.Millisecond,
			maxAge:             5 * time.Minute,
		},
		{
			name:               "fast_then_slow_timeout_then_fast",
			functionReturns:    []int{42, 88, 1},
			executionDurations: []time.Duration{1 * time.Millisecond, 100 * time.Millisecond, 2 * time.Millisecond},
			expectedResults:    []int{42, 42, 1},
			timeout:            50 * time.Millisecond,
			maxAge:             5 * time.Minute,
		},
		{
			name:               "stale_cache_fast_then_slow_timeout",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{10 * time.Millisecond, 100 * time.Millisecond},
			expectedResults:    []int{42, 88},
			timeout:            50 * time.Millisecond,
			maxAge:             30 * time.Millisecond,
		},
		{
			name:               "stale_cache_slow_timeout_then_slow_timeout",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{100 * time.Millisecond, 100 * time.Millisecond},
			expectedResults:    []int{42, 88},
			timeout:            50 * time.Millisecond,
			maxAge:             30 * time.Millisecond,
		},
		{
			name:               "stale_cache_fast_then_slow_timeout_then_fast_then_slow",
			functionReturns:    []int{42, 88, 1, 33},
			executionDurations: []time.Duration{1 * time.Millisecond, 100 * time.Millisecond, 2 * time.Millisecond, 100 * time.Millisecond},
			expectedResults:    []int{42, 88, 1, 33},
			timeout:            50 * time.Millisecond,
			maxAge:             30 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			numGoRoutineAtRest := runtime.NumGoroutine()
			endTime := time.Now()

			var i int
			fn := func() interface{} {
				result := tt.functionReturns[i]
				time.Sleep(tt.executionDurations[i])
				return result
			}
			decoratedFunc := TimeBoundWithCacheFunc(fn, tt.timeout, tt.maxAge)
			var cacheTimestamp time.Time

			for i = range tt.executionDurations {
				// compute end time for all function executions of the test
				tolerance := 1 * time.Millisecond
				finishesAt := time.Now().Add(tt.executionDurations[i] + tolerance)
				if finishesAt.After(endTime) {
					endTime = finishesAt
				}

				beforeExecution := time.Now()
				result := decoratedFunc()
				executionDuration := time.Now().Sub(beforeExecution)

				// save last time a result was cached
				if tt.executionDurations[i] <= tt.timeout {
					cacheTimestamp = time.Now()
				}

				// check that function execution took around tt.timeout
				// if the function does not have a cached result or the cache is stale due to maxAge, the timeout is not enforced
				cacheIsStale := time.Now().After(cacheTimestamp.Add(tt.maxAge))
				if !cacheIsStale && executionDuration > tt.timeout+tolerance {
					t.Errorf("iteration %v: decoratedFunc() took too long to execute %v which is greater than timeout (%v)", i, executionDuration, tt.timeout)
				}

				if !reflect.DeepEqual(result, tt.expectedResults[i]) {
					t.Errorf("iteration %v: decoratedFunc() = %v, want %v", i, result, tt.expectedResults[i])
				}
			}

			// detects if any goroutine is leaked after all processing is supposed to be complete
			time.Sleep(time.Until(endTime))
			extraGoRoutines := runtime.NumGoroutine() - numGoRoutineAtRest
			if extraGoRoutines > 0 {
				t.Errorf("too many goroutines left after processing: %d", extraGoRoutines)
			}
		})
	}
}
