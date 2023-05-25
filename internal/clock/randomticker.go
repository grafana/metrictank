package clock

import (
	"math/rand"
	"time"
)

// RandomTicker ticks at dur +- jitter
type RandomTicker struct {
	C        chan time.Time
	stopc    chan struct{}
	dur      time.Duration
	jitt     time.Duration
	blocking bool
}

// NewRandomTicker returns a new RandomTicker containing a channel that will send the time with a period specified by the duration and jitter arguments.
// It adjusts the intervals or drops ticks to make up for slow receivers. The duration and jitter must be greater than zero; if not, NewTicker will panic.
// duration must be >= jitter.
// Stop the ticker to release associated resources.
func NewRandomTicker(dur, jitt time.Duration, blocking bool) *RandomTicker {
	rt := RandomTicker{
		C:        make(chan time.Time),
		stopc:    make(chan struct{}),
		dur:      dur,
		jitt:     jitt,
		blocking: blocking,
	}
	go rt.run()
	return &rt
}

// Stop turns off the ticker. After Stop, no more ticks will be sent.
// Stop does not close the channel, to prevent a concurrent goroutine reading from the channel from seeing an erroneous "tick".
func (rt *RandomTicker) Stop() {
	rt.stopc <- struct{}{}
	<-rt.stopc
}

func (rt *RandomTicker) run() {
	for {
		t := time.NewTimer(rt.duration())
		select {
		case <-rt.stopc:
			t.Stop()
			close(rt.stopc)
			return
		case <-t.C:
			if rt.blocking {
				rt.C <- time.Now()
			} else {
				select {
				case rt.C <- time.Now():
				default:
				}
			}
			t.Stop()
		}
	}
}

func (rt *RandomTicker) duration() time.Duration {
	if rt.jitt > 0 {
		return rt.dur + time.Duration(rand.Int63n(int64(2*rt.jitt))) - rt.jitt
	}
	return rt.dur
}
