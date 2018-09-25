package memory

import (
	"context"
	"sync"
	"time"
)

// TimeLimiter provides a means limit the amount of time spent working.TimeLimiter
type TimeLimiter struct {
	sync.Mutex
	ctx          context.Context
	lockDuration time.Duration
	window       time.Duration
	limit        time.Duration
	accountCh    chan time.Duration
	queryCh      chan chan struct{}
}

// NewTimeLimiter creates a new TimeLimiter.  A background thread will run until the
// provided context is done.  When the amount of time spent on task (the time is determined
// by calls to "Add()") every "window" duration is more then "limit",  then calls to
// Wait() will block until the start if the next window period.
func NewTimeLimiter(ctx context.Context, window, limit time.Duration) *TimeLimiter {
	l := &TimeLimiter{
		ctx:       ctx,
		window:    window,
		limit:     limit,
		accountCh: make(chan time.Duration),
		queryCh:   make(chan chan struct{}),
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
			blockedQueries = nil
			return
		case <-ticker.C:
			// reset lockDuration to 0
			l.lockDuration = time.Duration(0)
			for _, ch := range blockedQueries {
				close(ch)
			}
			blockedQueries = nil
		case d := <-l.accountCh:
			l.lockDuration += d
		case respCh := <-l.queryCh:
			if l.lockDuration < l.limit {
				close(respCh)
			} else {
				// rate limit exceeded.  On the next tick respCh will be closed
				// notifying the caller that they can continue.
				blockedQueries = append(blockedQueries, respCh)
			}
		}
	}
}

// Add increments the counter of time spent doing something by "d"
func (l *TimeLimiter) Add(d time.Duration) {
	l.accountCh <- d
}

// Wait will return immediately if we are not rate limited, otherwise it will
// block until we are no longer limited.  The longest we will block for is
// the size of the defined time window.
func (l *TimeLimiter) Wait() {
	respCh := make(chan struct{})
	l.queryCh <- respCh

	// if we have not exceeded our locking quota then respCh will be
	// immediately closed. Otherwise it wont be closed until the next tick (duration of "l.window")
	// and we will block until then.
	<-respCh
}
