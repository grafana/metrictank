package util

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLimiter(t *testing.T) {

	ctx := context.Background()
	Convey("when limiter below limits", t, func() {
		limiter := NewLimiter(1)
		start := time.Now()
		ok := limiter.Acquire(ctx)
		So(ok, ShouldBeTrue)
		So(time.Now(), ShouldHappenWithin, time.Millisecond, start)
		Convey("when limiter reaches limit", func() {
			ch := make(chan bool)
			ctx, cancel := context.WithCancel(ctx)
			go func(ctx context.Context) {
				ch <- limiter.Acquire(ctx)
			}(ctx)
			blocked := true
			select {
			case <-time.After(time.Millisecond):
			case <-ch:
				blocked = false
			}
			So(blocked, ShouldBeTrue)
			Convey("when worker released", func() {
				limiter.Release()
				ok := <-ch
				So(ok, ShouldBeTrue)
			})
			Convey("when context canceled", func() {
				start := time.Now()
				cancel()
				ok := <-ch
				So(ok, ShouldBeFalse)
				So(time.Now(), ShouldHappenWithin, time.Millisecond, start)
			})
		})
	})
	Convey("when context canceled before calling Acquire", t, func() {
		limiter := NewLimiter(1)
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		ok := limiter.Acquire(ctx)
		So(ok, ShouldBeFalse)
	})
}
