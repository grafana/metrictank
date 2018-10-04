package memory

import (
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
)

// TimeLimiter limits the rate of a set of serial operations.
// It does this by tracking how much time has been spent (updated via Add()),
// and comparing this to the window size and the limit, slowing down further operations as soon
// as one Add() is called informing it the per-window allowed budget has been exceeded.
// Limitations:
// * the last operation is allowed to exceed the budget (but the next call will be delayed to compensate)
// * concurrency is not supported
//
// For correctness, you should always follow up an Add() with a Wait()
type TimeLimiter struct {
	since     time.Time
	next      time.Time
	timeSpent time.Duration
	window    time.Duration
	limit     time.Duration
}

// NewTimeLimiter creates a new TimeLimiter.
func NewTimeLimiter(window, limit time.Duration, now time.Time) *TimeLimiter {
	l := TimeLimiter{
		since:  now,
		next:   now.Add(window),
		window: window,
		limit:  limit,
	}
	spew.Dump(l)
	return &l
}

// Add increments the "time spent" counter by "d"
func (l *TimeLimiter) Add(d time.Duration) {
	l.add(time.Now(), d)
}

// add increments the "time spent" counter by "d" at a given time
func (l *TimeLimiter) add(now time.Time, d time.Duration) {
	if now.After(l.next) {
		l.timeSpent = d
		l.since = now.Add(-d)
		l.next = l.since.Add(l.window)
		fmt.Println("added and updated")
		spew.Dump(l)
		return
	}
	l.timeSpent += d
}

// Wait returns when we are not rate limited
// * if we passed the window, we reset everything (this is only safe for callers
//     that behave correctly, i.e. that wait the instructed time after each add)
// * if limit is not reached, no sleep is needed
// * if limit has been exceeded, sleep until next period + extra multiple to compensate
//    this is perhaps best explained with an example:
//    if window is 1s and limit 100ms, but we spent 250ms, then we spent effectively 2.5 seconds worth of work.
//    let's say we are 800ms into the 1s window, that means we should sleep 2500-800 = 1.7s
//    in order to maximize work while honoring the imposed limit.
// * if limit has been met exactly, sleep until next period (this is a special case of the above)
func (l *TimeLimiter) Wait() {
	time.Sleep(l.wait(time.Now()))
}

// wait returns how long should be slept at a given time. See Wait() for more info
func (l *TimeLimiter) wait(now time.Time) time.Duration {

	// if we passed the window, reset and start over
	// if clock is adjusted backwards, best we can do is also just reset and start over
	if now.After(l.next) || now.Before(l.since) {
		l.timeSpent = 0
		l.since = now
		l.next = now.Add(l.window)
		fmt.Println("wait and update")
		spew.Dump(l)
		return 0
	}
	if l.timeSpent < l.limit {
		return 0
	}

	// now <= next
	// now >= since
	// timespent >= limit
	excess := l.timeSpent - l.limit
	multiplier := l.window / l.limit
	timeToPass := excess * multiplier
	timePassed := now.Sub(l.since)
	// not sure if this should happen, but let's be safe anyway
	if timePassed >= timeToPass {
		return 0
	}
	fmt.Println("wait and now is", now)
	return timeToPass - timePassed
}
