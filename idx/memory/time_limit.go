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
	sync.Mutex
	ctx       context.Context
	timeSpent time.Duration
	window    time.Duration
	limit     time.Duration
	addCh     chan time.Duration
	queryCh   chan chan struct{}
}

// NewTimeLimiter creates a new TimeLimiter.  A background goroutine will run until the
// provided context is done.  When the amount of time spent on task (the time is determined
// by calls to "Add()") every "window" duration is more then "limit",  then calls to
// Wait() will block until the start if the next window period.
func NewTimeLimiter(ctx context.Context, window, limit time.Duration) *TimeLimiter {
	l := &TimeLimiter{
		ctx:     ctx,
		window:  window,
		limit:   limit,
		addCh:   make(chan time.Duration),
		queryCh: make(chan chan struct{}),
	}
	go l.run()
	return l
}

func (l *TimeLimiter) run() {
	ticker := time.NewTicker(l.window)
	done := l.ctx.Done()
	var blockedQueries []chan struct{}
	for {
		select {
		case <-done:
			//context done. shutting down
			for _, ch := range blockedQueries {
				close(ch)
			}
			return
		case <-ticker.C:
			l.timeSpent = 0
			for _, ch := range blockedQueries {
				close(ch)
			}
			blockedQueries = blockedQueries[:0]
		case d := <-l.addCh:
			l.timeSpent += d
		case respCh := <-l.queryCh:
			if l.timeSpent < l.limit {
				close(respCh)
			} else {
				// rate limit exceeded.  On the next tick respCh will be closed
				// notifying the caller that they can continue.
				blockedQueries = append(blockedQueries, respCh)
			}
		}
	}
}

// Add increments the "time spent" counter by "d"
func (l *TimeLimiter) Add(d time.Duration) {
	l.addCh <- d
}

// Wait returns when we are not rate limited, which may be
// anywhere between immediately or after the window.
func (l *TimeLimiter) Wait() {
	respCh := make(chan struct{})
	l.queryCh <- respCh

	// if we have not exceeded our locking quota then respCh will be
	// immediately closed. Otherwise it wont be closed until the next tick (duration of "l.window")
	// and we will block until then.
	<-respCh
}
