// Package clock provides aligned tickers.
// An aligned ticker is a channel of time.Time "ticks" similar to time.Ticker,
// but the ticks are even multiples of the requested period, and are delivered
// as shortly as possible after the clock reaching these timestamps.
// For example, with period=10s, the ticker ticks shortly after the passing of a unix
// timestamp that is a multiple of 10s, and the values returned are always these multiples.
// In my testing it practically ticks about .0001 to 0.0002 seconds later due to scheduling etc,
// but under high load, the delta may be larger. The ticks are always the "ideal" values
package clock

import "time"

// AlignedTickLossy returns an aligned ticker that may drop ticks
// (if the consumer is slow or the clock jumps forward)
func AlignedTickLossy(period time.Duration) <-chan time.Time {
	c := make(chan time.Time)
	go func() {
		for {
			now := time.Now()
			nowUnix := now.UnixNano()
			diff := period - (time.Duration(nowUnix) % period)
			ideal := now.Add(diff)
			time.Sleep(diff)
			select {
			case c <- ideal:
			default:
			}
		}
	}()
	return c
}

// AlignedTickLossless returns an aligned ticker that waits for slow receivers,
// and backfills later as necessary to publish any pending ticks, at possibly
// a much more aggressive schedule. (keeps ticking until fully caught up)
// Note: clock jumps may still result in dropped ticks.
func AlignedTickLossless(period time.Duration) <-chan time.Time {
	c := make(chan time.Time)
	nsec := (time.Now().UnixNano() / int64(period)) * int64(period)
	next := time.Unix(0, nsec).Add(period)
	go func() {
		for {
			now := time.Now()

			// handle catch up / backfill, if the consumer has run behind the real clock
			for now.After(next) {
				c <- next
				next = next.Add(period)
				now = time.Now()
			}

			// now that we're caught up, sleep until the clock reaches the next tick
			diff := next.Sub(now)
			time.Sleep(diff)
			c <- next
			next = next.Add(period)
		}
	}()
	return c
}
