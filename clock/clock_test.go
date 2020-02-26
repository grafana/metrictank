package clock

import (
	"testing"
	"time"
)

func TestAlignedTickLossless(t *testing.T) {
	period := 100 * time.Millisecond
	lateToleration := 10 * time.Millisecond

	// whatever time it is now, we expect the tick pointing to the next aligned tick,
	// and we expect to see it slightly after that timestamp
	tick := AlignedTickLossless(period)
	now := time.Now()
	expTick := time.Unix(0, (1+now.UnixNano()/int64(period))*int64(period))
	expWithin := expTick.Sub(now)
	select {
	case v0 := <-tick:
		if v0 != expTick {
			t.Fatalf("expected v0 %v, got %v", expTick, v0)
		}
		elapsed := time.Since(now)
		if elapsed < expWithin || elapsed > expWithin+lateToleration {
			t.Fatalf("expected to see tick v0 after %v to %v, but it took %v", expWithin, expWithin+lateToleration, elapsed)
		}
	case <-time.After(period + 10*time.Millisecond):
		t.Fatalf("did not get tick v0 on time")
	}

	// sleep for 3 periods, we should then get these ticks immediately once we start consuming them
	time.Sleep(3 * period)

	expTick = expTick.Add(period)
	select {
	case v1 := <-tick:
		if v1 != expTick {
			t.Fatalf("expected v1 %v, got %v", expTick, v1)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("did not get tick v1 on time")
	}

	expTick = expTick.Add(period)
	select {
	case v2 := <-tick:
		if v2 != expTick {
			t.Fatalf("expected v2 %v, got %v", expTick, v2)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("did not get tick v2 on time")
	}

	expTick = expTick.Add(period)
	select {
	case v3 := <-tick:
		if v3 != expTick {
			t.Fatalf("expected v3 %v, got %v", expTick, v3)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("did not get tick v3 on time")
	}

	// our next read should wait until it becomes available, which is about one period from now
	// (could be less due to v3 having been tolerably late, or could be more due to v4 being
	// tolerably late)
	now = time.Now()
	expTick = expTick.Add(period)
	expWithin = period
	select {
	case v4 := <-tick:
		if v4 != expTick {
			t.Fatalf("expected v4 %v, got %v", expTick, v4)
		}
		elapsed := time.Since(now)
		if elapsed < expWithin-lateToleration || elapsed > expWithin+lateToleration {
			t.Fatalf("expected to see tick v4 after %v to %v, but it took %v", expWithin-lateToleration, expWithin+lateToleration, elapsed)
		}
	case <-time.After(period + 10*time.Millisecond):
		t.Fatalf("did not get tick v4 on time")
	}

}
