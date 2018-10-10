package memory

import (
	"testing"
	"time"
)

var now = time.Unix(10, 0)

func shouldTake(t *testing.T, tl *TimeLimiter, workDone, expDur time.Duration, info string) {

	// account for work done, as well as moving our clock forward by the same amount
	now = now.Add(workDone)
	tl.add(now, workDone)

	dur := tl.wait(now)
	if dur != expDur {
		t.Fatalf("scenario %s. expected wait %s, got wait %s", info, expDur, dur)
	}

	// fake the "sleep" so we're a properly behaving caller
	now = now.Add(dur)
}

func TestTimeLimiter(t *testing.T) {
	window := time.Second
	limit := 100 * time.Millisecond

	tl := NewTimeLimiter(window, limit, now)

	// TEST 1 : Start first window by doing work and seeing when it starts blocking
	shouldTake(t, tl, 0, 0, "window 1: work done: 0")
	shouldTake(t, tl, 5*time.Millisecond, 0, "window 1: work done: 5ms")
	shouldTake(t, tl, 10*time.Millisecond, 0, "window 1: work done: 15ms")
	shouldTake(t, tl, 80*time.Millisecond, 0, "window 1: work done: 95ms")
	shouldTake(t, tl, 4*time.Millisecond, 0, "window 1: work done: 99ms")
	shouldTake(t, tl, 3*time.Millisecond, (1020-102)*time.Millisecond, "window 1: work done: 102ms")

	// TEST 2 : Now that we waited until a full window, should be able to up to limit work again
	shouldTake(t, tl, 50*time.Millisecond, 0, "window 2: work done: 50ms")
	shouldTake(t, tl, 40*time.Millisecond, 0, "window 2: work done: 90ms")
	shouldTake(t, tl, 40*time.Millisecond, (1300-130)*time.Millisecond, "window 2: work done: 130ms")
}
