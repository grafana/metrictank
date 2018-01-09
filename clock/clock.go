package clock

import "time"

// AlignedTick returns a tick channel so that, let's say interval is a second
// then it will tick at every whole second, or if it's 60s than it's every whole
// minute. Note that in my testing this is about .0001 to 0.0002 seconds later due
// to scheduling etc.
func AlignedTick(period time.Duration) <-chan time.Time {
	// note that time.Ticker is not an interface,
	// and that if we instantiate one, we can't write to its channel
	// hence we can't leverage that type.
	c := make(chan time.Time)
	go func() {
		for {
			unix := time.Now().UnixNano()
			diff := time.Duration(period - (time.Duration(unix) % period))
			time.Sleep(diff)
			select {
			case c <- time.Now():
			default:
			}
		}
	}()
	return c
}
