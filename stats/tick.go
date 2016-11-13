package stats

import "time"

// provides "clean" ticks at precise intervals, and delivers them shortly after
func tick(period time.Duration) chan time.Time {
	ch := make(chan time.Time)
	go func() {
		for {
			now := time.Now()
			nowUnix := now.UnixNano()
			diff := period - (time.Duration(nowUnix) % period)
			ideal := now.Add(diff)
			time.Sleep(diff)

			// try to write, if it blocks, skip the tick
			select {
			case ch <- ideal:
			default:
			}
		}
	}()
	return ch
}
