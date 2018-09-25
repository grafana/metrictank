package memory

import (
	"context"
	"testing"
	"time"
)

func shouldTakeAbout(t *testing.T, fn func(), expDur time.Duration, sloppynessFactor int, info string) {
	// on Dieter's laptop:
	// takes about <=15 micros for add/wait sequences
	// takes about 150micros for a add + blocking wait
	// on circleCI, takes 75micros for add/wait sequence
	slop := time.Duration(sloppynessFactor) * time.Microsecond
	pre := time.Now()
	fn()
	dur := time.Since(pre)
	if dur > expDur+slop || dur < expDur-slop {
		t.Fatalf("scenario %s. was supposed to take %s, but took %s", info, expDur, dur)
	}
}

func TestTimeLimiter(t *testing.T) {
	window := time.Second
	limit := 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	tl := NewTimeLimiter(ctx, window, limit)

	// TEST 1 : Start first window by doing work and seeing when it starts blocking
	shouldTakeAbout(t, tl.Wait, 0, 100, "window 1: work done: 0 - wait should be 0")

	tl.Add(5 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 100, "window 1: work done: 5ms - wait should be 0")

	tl.Add(10 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 100, "window 1: work done: 15ms - wait should be 0")

	tl.Add(80 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 100, "window 1: work done: 95ms - wait should be 0")

	tl.Add(4 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 100, "window 1: work done: 99ms - wait should be 0")

	tl.Add(3 * time.Millisecond)

	shouldTakeAbout(t, tl.Wait, time.Second, 500, "window 1: work done: 102ms - almost no time has passed, so wait should be full window")

	// TEST 2 : Now that we waited until a full window, should be able to up to limit work again
	tl.Add(50 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 100, "window 2: work done: 50ms - wait should be 0")

	tl.Add(40 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 100, "window 2: work done: 90ms - wait should be 0")

	tl.Add(40 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, time.Second, 500, "window 2: work done: 130ms - wait should be 1s")

	// TEST 3 : Now that we waited until a full window, should be able to up to limit work again
	// but this time we cancel, so we don't have to wait as long
	tl.Add(50 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 100, "window 3: work done: 50ms - wait should be 0")

	tl.Add(40 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 100, "window 3: work done: 90ms - wait should be 0")

	tl.Add(40 * time.Millisecond)

	time.AfterFunc(500*time.Millisecond, cancel)
	shouldTakeAbout(t, tl.Wait, 500*time.Millisecond, 500, "window 3: work done: 130ms, canceling after 500ms - wait should be 500ms")
}
