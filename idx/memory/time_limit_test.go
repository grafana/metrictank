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
	slop := time.Duration(sloppynessFactor) * time.Microsecond
	pre := time.Now()
	fn()
	dur := time.Since(pre)
	if dur > expDur+slop || dur < expDur-slop {
		t.Fatalf("scenario %s. was supposed to take %s, but took %s", info, expDur, dur)
	}
}

func TestTimeLimiter(t *testing.T) {
	window := 100 * time.Millisecond
	limit := 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	tl := NewTimeLimiter(ctx, window, limit)

	// TEST 1 : Start first window by doing work and seeing when it starts blocking
	shouldTakeAbout(t, tl.Wait, 0, 50, "window 1: work done: 0 - wait should be 0")

	tl.Add(500 * time.Microsecond)
	shouldTakeAbout(t, tl.Wait, 0, 50, "window 1: work done: 500micros - wait should be 0")

	tl.Add(time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 50, "window 1: work done: 1.5ms - wait should be 0")

	tl.Add(8 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 50, "window 1: work done: 9.5ms - wait should be 0")

	tl.Add(400 * time.Microsecond)
	shouldTakeAbout(t, tl.Wait, 0, 50, "window 1: work done: 9.9ms - wait should be 0")

	tl.Add(300 * time.Microsecond)

	shouldTakeAbout(t, tl.Wait, 100*time.Millisecond, 500, "window 1: work done: 10.2ms - almost no time has passed, so wait should be full window")

	// TEST 2 : Now that we waited until a full window, should be able to up to limit work again
	tl.Add(5 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 50, "window 2: work done: 5ms - wait should be 0")

	tl.Add(4 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 50, "window 2: work done: 9ms - wait should be 0")

	tl.Add(4 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 100*time.Millisecond, 500, "window 2: work done: 13ms - wait should be 100ms")

	// TEST 3 : Now that we waited until a full window, should be able to up to limit work again
	// but this time we cancel, so we don't have to wait as long
	tl.Add(5 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 50, "window 3: work done: 5ms - wait should be 0")

	tl.Add(4 * time.Millisecond)
	shouldTakeAbout(t, tl.Wait, 0, 50, "window 3: work done: 9ms - wait should be 0")

	tl.Add(4 * time.Millisecond)

	time.AfterFunc(50*time.Millisecond, cancel)
	shouldTakeAbout(t, tl.Wait, 50*time.Millisecond, 500, "window 3: work done: 13ms, canceling after 50ms - wait should be 50ms")
}
