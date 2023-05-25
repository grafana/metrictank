package util

import (
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestTimeBoundWithCacheFunc(t *testing.T) {
	testMachineLatency := 50 * time.Millisecond

	zeroDuration := 0 * time.Millisecond
	veryShortDuration := 1 * time.Millisecond
	shortDuration := veryShortDuration + testMachineLatency*2
	longDuration := shortDuration + testMachineLatency*2
	veryLongDuration := longDuration + testMachineLatency*2

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
			executionDurations: []time.Duration{zeroDuration},
			expectedResults:    []int{42},
			timeout:            shortDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "immediate_zero_timeout",
			functionReturns:    []int{42},
			executionDurations: []time.Duration{zeroDuration},
			expectedResults:    []int{42},
			timeout:            zeroDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "immediate_zero_maxage",
			functionReturns:    []int{42},
			executionDurations: []time.Duration{zeroDuration},
			expectedResults:    []int{42},
			timeout:            shortDuration,
			maxAge:             zeroDuration,
		},
		{
			name:               "slow_notimeout",
			functionReturns:    []int{42},
			executionDurations: []time.Duration{shortDuration},
			expectedResults:    []int{42},
			timeout:            longDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "slow_timeout",
			functionReturns:    []int{42},
			executionDurations: []time.Duration{longDuration},
			expectedResults:    []int{42},
			timeout:            shortDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "fast_then_slow_timeout",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{veryShortDuration, longDuration},
			expectedResults:    []int{42, 42},
			timeout:            shortDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "fast_then_fast",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{veryShortDuration, veryShortDuration},
			expectedResults:    []int{42, 88},
			timeout:            shortDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "slow_timeout_then_slow_timeout",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{longDuration, longDuration},
			expectedResults:    []int{42, 42},
			timeout:            shortDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "fast_then_fast_then_slow_timeout",
			functionReturns:    []int{42, 88, 1},
			executionDurations: []time.Duration{veryShortDuration, veryShortDuration, longDuration},
			expectedResults:    []int{42, 88, 88},
			timeout:            shortDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "fast_then_slow_then_fast_never_timeout",
			functionReturns:    []int{42, 88, 1},
			executionDurations: []time.Duration{veryShortDuration, longDuration, veryShortDuration},
			expectedResults:    []int{42, 88, 1},
			timeout:            veryLongDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "fast_then_slow_timeout_then_fast",
			functionReturns:    []int{42, 88, 1},
			executionDurations: []time.Duration{veryShortDuration, longDuration, veryShortDuration},
			expectedResults:    []int{42, 42, 1},
			timeout:            shortDuration,
			maxAge:             veryLongDuration,
		},
		{
			name:               "stale_cache_fast_then_slow_timeout",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{veryShortDuration, veryLongDuration},
			expectedResults:    []int{42, 88},
			timeout:            longDuration,
			maxAge:             shortDuration,
		},
		{
			name:               "stale_cache_slow_timeout_then_slow_timeout",
			functionReturns:    []int{42, 88},
			executionDurations: []time.Duration{veryLongDuration, veryLongDuration},
			expectedResults:    []int{42, 88},
			timeout:            longDuration,
			maxAge:             shortDuration,
		},
		{
			name:               "stale_cache_fast_then_slow_timeout_then_fast_then_slow",
			functionReturns:    []int{42, 88, 1, 33},
			executionDurations: []time.Duration{veryShortDuration, veryLongDuration, veryShortDuration, veryLongDuration},
			expectedResults:    []int{42, 88, 1, 33},
			timeout:            longDuration,
			maxAge:             shortDuration,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			numGoRoutineAtRest := runtime.NumGoroutine()
			endTime := time.Now()

			functionReturnChan := make(chan int, 1)
			executionDurationChan := make(chan time.Duration, 1)
			fn := func() interface{} {
				time.Sleep(<-executionDurationChan)
				return <-functionReturnChan
			}
			decoratedFunc := TimeBoundWithCacheFunc(fn, tt.timeout, tt.maxAge)
			var cacheTimestamp time.Time

			for i := range tt.executionDurations {
				// compute end time for all function executions of the test
				finishesAt := time.Now().Add(tt.executionDurations[i])
				if finishesAt.After(endTime) {
					endTime = finishesAt
				}

				functionReturnChan <- tt.functionReturns[i]
				executionDurationChan <- tt.executionDurations[i]
				beforeExecution := time.Now()
				result := decoratedFunc()
				executionDuration := time.Now().Sub(beforeExecution)

				// check that function execution took around tt.timeout
				// if the function does not have a cached result or the cache is stale due to maxAge, the timeout is not enforced
				cacheIsStale := time.Now().After(cacheTimestamp.Add(tt.maxAge))
				if !cacheIsStale && executionDuration > tt.timeout+testMachineLatency {
					t.Errorf("iteration %v: decoratedFunc() took too long to execute %v which is greater than timeout (%v)", i, executionDuration, tt.timeout)
				}

				if !reflect.DeepEqual(result, tt.expectedResults[i]) {
					t.Errorf("iteration %v: decoratedFunc() = %v, want %v", i, result, tt.expectedResults[i])
				}

				// save last time a result was cached
				if tt.executionDurations[i] <= tt.timeout {
					cacheTimestamp = time.Now()
				}
			}

			// detects if any goroutine is leaked after all processing is supposed to be complete
			time.Sleep(time.Until(endTime.Add(testMachineLatency)))
			extraGoRoutines := runtime.NumGoroutine() - numGoRoutineAtRest
			if extraGoRoutines > 0 {
				t.Errorf("too many goroutines left after processing: %d", extraGoRoutines)
			}
		})
	}
}
