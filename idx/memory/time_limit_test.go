package memory

import (
	"context"
	"testing"
	"time"
)

func shouldTakeAbout(t *testing.T, fn func(), expDur time.Duration, sloppynessFactor int) {
	// on Dieter's laptop:
	// takes about <=15 micros for add/wait sequences
	// takes about 150micros for a add + blocking wait
	slop := time.Duration(sloppynessFactor) * time.Microsecond
	pre := time.Now()
	fn()
	dur := time.Since(pre)
	if dur > expDur+slop || dur < expDur-slop {
		t.Fatalf("was supposed to take %s, but took %s", expDur, dur)
	}
}

func TestTimeLimiter(t *testing.T) {
	window := 100 * time.Millisecond
	limit := 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	tl := NewTimeLimiter(ctx, window, limit)

	// TEST 1 : Start first window by doing work and seeing when it starts blocking
	shouldTakeAbout(t, tl.Wait, 0, 50)

	tl.Add(500 * time.Microsecond) // spent: 500micros
	shouldTakeAbout(t, tl.Wait, 0, 50)

	tl.Add(time.Millisecond) // spent: 1.5 ms
	shouldTakeAbout(t, tl.Wait, 0, 50)

	tl.Add(8 * time.Millisecond) // spent: 9.5 ms
	shouldTakeAbout(t, tl.Wait, 0, 50)

	tl.Add(400 * time.Microsecond) // spent: 9.9 ms
	shouldTakeAbout(t, tl.Wait, 0, 50)

	tl.Add(300 * time.Microsecond) // spent: 10.2 ms

	// since almost no time has passed, have to wait full window
	shouldTakeAbout(t, tl.Wait, 100*time.Millisecond, 500)

	// TEST 2 : Now that we waited until a full window, should be able to up to limit work again
	tl.Add(5 * time.Millisecond) // spent: 5 ms
	shouldTakeAbout(t, tl.Wait, 0, 50)

	tl.Add(4 * time.Millisecond) // spent: 9 ms
	shouldTakeAbout(t, tl.Wait, 0, 50)

	tl.Add(4 * time.Millisecond) // spent: 13 ms
	shouldTakeAbout(t, tl.Wait, 100*time.Millisecond, 500)

	// TEST 3 : Now that we waited until a full window, should be able to up to limit work again
	// but this time we cancel, so we don't have to wait as long
	tl.Add(5 * time.Millisecond) // spent: 5 ms
	shouldTakeAbout(t, tl.Wait, 0, 50)

	tl.Add(4 * time.Millisecond) // spent: 9 ms
	shouldTakeAbout(t, tl.Wait, 0, 50)

	tl.Add(4 * time.Millisecond) // spent: 13 ms

	time.AfterFunc(50*time.Millisecond, cancel)
	shouldTakeAbout(t, tl.Wait, 50*time.Millisecond, 500)
}
