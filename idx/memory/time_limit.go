package memory

import (
	"context"
	"sync"
	"time"
)

// TimeLimiter limits the rate of a set of operations.
// It does this by slowing down further operations as soon
// as one Add() is called informing it the per-window allowed budget has been exceeded.
// Limitations:
// * concurrently running operations can all exceed the budget,
//   so it works best for serial operations.
// * for serial operations, the last operation is allowed to exceed the budget
// * when an operation takes very long (e.g. 10 seconds, with a 100ms limit per second), it
//   is counted as exceeding the 100ms budget, but no other provisions are being made.
//
// Thus, TimeLimiter is designed for, and works best with, serially running operations,
// each of which takes a fraction of the limit.
type TimeLimiter struct {
	sync.RWMutex
	ctx       context.Context
	timeSpent time.Duration // cummulative time spent in the current window
	window    time.Duration // size of the window
	limit     time.Duration // maximum timeSpent value before blocking.
	wg        sync.WaitGroup
	limited   bool
	stopped   bool
}

// NewTimeLimiter creates a new TimeLimiter. When the amount of time spent on task
// (the time is determined by calls to "Add()") every "window" duration is more then "limit",
// then calls to Wait() will block until the start if the next window period.
// The limiter will continue to run until the passed context is done.  Any calls to Wait() after
// the context is done will immediate return.
func NewTimeLimiter(ctx context.Context, window, limit time.Duration) *TimeLimiter {
	l := &TimeLimiter{
		ctx:    ctx,
		window: window,
		limit:  limit,
	}
	go l.run()
	return l
}

// core loop.  At every `window` interval will reset timeSpent to 0
// and unblock any callers to Wait() if we had reached the limit during the window.
func (l *TimeLimiter) run() {
	done := l.ctx.Done()
	ticker := time.NewTicker(l.window)
	for {
		select {
		case <-done:
			ticker.Stop()
			l.Lock()
			l.stopped = true
			// if we were limited, then unblock anyone waiting
			if l.limited {
				l.wg.Done()
				l.limited = false
			}
			l.Unlock()
			return
		case <-ticker.C:
			l.Lock()
			// reset timeSpent
			l.timeSpent = 0

			// if we were limited, then unblock anyone waiting
			if l.limited {
				l.wg.Done()
				l.limited = false
			}
			l.Unlock()
		}
	}
}

// ensure that any callers to Wait() will be blocked.
// Assumes the caller has a write Lock acquired with l.Lock()
func (l *TimeLimiter) block() {
	if l.limited || l.stopped {
		return
	}
	l.limited = true
	l.wg.Add(1)
}

// Add increments the "time spent" counter by "d"
// If after adding the limit is reached, any new callers to Wait() will be blocked.
func (l *TimeLimiter) Add(d time.Duration) {
	l.Lock()
	l.timeSpent = l.timeSpent + d
	if l.timeSpent > l.limit {
		l.block()
	}
	l.Unlock()
}

// Wait returns when we are not rate limited, which may be
// anywhere between immediately or after the window.
func (l *TimeLimiter) Wait() {
	l.wg.Wait()
	return
}
