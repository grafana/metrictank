package kafkamdm

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLagLogger(t *testing.T) {
	logger := newLagLogger(5)
	now := time.Now()

	Convey("with 0 measurements", t, func() {
		So(logger.Min(), ShouldEqual, -1)
	})
	Convey("with 1 measurements", t, func() {
		logger.Store(0, 10, now.Add(time.Second*time.Duration(1)))
		So(logger.Min(), ShouldEqual, 10)
		So(logger.Rate(), ShouldEqual, 0)
	})
	Convey("with 2 measurements", t, func() {
		logger.Store(10, 15, now.Add(time.Second*time.Duration(2)))
		So(logger.Min(), ShouldEqual, 5)
		So(logger.Rate(), ShouldEqual, 5)
	})
	Convey("with a negative measurement", t, func() {
		logger.Store(10, 5, now.Add(time.Second*time.Duration(3)))

		// Negative measuremnets are discarded, should be same as last time.
		So(logger.Min(), ShouldEqual, 5)
		So(logger.Rate(), ShouldEqual, 5)
	})
	Convey("with lots of measurements", t, func() {
		for i := 0; i < 100; i++ {
			logger.Store(int64(10+i), int64(15+2*i), now.Add(time.Second*time.Duration(3+i)))
		}
		So(logger.Min(), ShouldEqual, 100)
		So(logger.Rate(), ShouldEqual, 2)
	})
}

/*
func TestRateLogger(t *testing.T) {
	logger := newRateLogger()
	now := time.Now()
	Convey("with 0 measurements", t, func() {
		So(logger.Rate(), ShouldEqual, 0)
	})
	Convey("after 1st measurements", t, func() {
		logger.Store(10, now)
		So(logger.Rate(), ShouldEqual, 0)
	})
	Convey("with 2nd measurements", t, func() {
		logger.Store(15, now.Add(time.Second))
		So(logger.Rate(), ShouldEqual, 5)
	})
	Convey("with old ts", t, func() {
		logger.Store(25, now)
		So(logger.Rate(), ShouldEqual, 5)
	})
	Convey("with less then 1per second", t, func() {
		logger.Store(30, now.Add(time.Second*10))
		So(logger.Rate(), ShouldEqual, 0)
	})
}

func TestRateLoggerSmallIncrements(t *testing.T) {
	logger := newRateLogger()
	now := time.Now()
	Convey("after 1st measurements", t, func() {
		logger.Store(10, now)
		So(logger.Rate(), ShouldEqual, 0)
	})
	Convey("with 2nd measurements", t, func() {
		logger.Store(20, now.Add(200*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 0)
	})
	Convey("with 3rd measurements", t, func() {
		logger.Store(30, now.Add(400*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 0)
	})
	Convey("with 4th measurements", t, func() {
		logger.Store(40, now.Add(600*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 0)
	})
	Convey("with 5th measurements", t, func() {
		logger.Store(50, now.Add(800*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 0)
	})
	Convey("with 6th measurements", t, func() {
		logger.Store(60, now.Add(1000*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 60-10)
	})
	Convey("with 7th measurements", t, func() {
		logger.Store(80, now.Add(1200*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 60-10)
	})
	Convey("with 8th measurements", t, func() {
		logger.Store(100, now.Add(1400*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 60-10)
	})
	Convey("with 9th measurements", t, func() {
		logger.Store(120, now.Add(1600*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 60-10)
	})
	Convey("with 10th measurements", t, func() {
		logger.Store(140, now.Add(1800*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 60-10)
	})
	Convey("with 11th measurements", t, func() {
		logger.Store(160, now.Add(2000*time.Millisecond))
		So(logger.Rate(), ShouldEqual, 160-60)
	})
}
*/

// TestLagMonitor tests LagMonitor priorities based on various scenarios.
// the overall priority is obviously simply the max priority of any of the partitions,
// so for simplicity we can focus on simulating 1 partition.
func TestLagMonitor(t *testing.T) {
	start := time.Now()
	mon := NewLagMonitor(2, []int32{0})

	// advance records a state change (newest and current offsets) for the given timestamp
	advance := func(sec int, newest, offset int64) {
		ts := start.Add(time.Second * time.Duration(sec))
		mon.StoreOffsets(0, offset, newest, ts)
	}

	Convey("with 0 measurements, priority should be 10k", t, func() {
		So(mon.Metric(), ShouldEqual, 10000)
		Reset(func() { mon = NewLagMonitor(2, []int32{0}) })
		Convey("with 100 measurements, not consuming and lag just growing", func() {
			for i := 0; i < 100; i++ {
				advance(i, int64(i), 0)
			}
			So(mon.Metric(), ShouldEqual, 98) // min-lag(98) / rate (0) = 98
		})
		Convey("with 100 measurements, each advancing 1 offset per second, and lag growing by 1 each second (e.g. real rate is 2/s)", func() {
			for i := 0; i < 100; i++ {
				advance(i, int64(i*2), int64(i))
			}
			So(mon.Metric(), ShouldEqual, 98) // min-lag(98) / rate (1) = 98
		})
		Convey("rate of production is 100k and lag is 1000", func() {
			advance(1, 100000, 99000)
			advance(2, 200000, 199000)
			So(mon.Metric(), ShouldEqual, 0) // 1000 / 100k = 0
			Convey("rate of production goes up to 200k but lag stays consistent at 1000", func() {
				advance(3, 400000, 399000)
				advance(4, 600000, 599000)
				So(mon.Metric(), ShouldEqual, 0) // 1000 / 200k = 0
			})
			Convey("rate of production goes down to 1000 but lag stays consistent at 1000", func() {
				advance(3, 201000, 200000)
				advance(4, 202000, 201000)
				So(mon.Metric(), ShouldEqual, 1) // 1000 / 1000 = 1
			})
			Convey("rate of production goes up to 200k but we can only keep up with the rate of 100k so lag starts growing", func() {
				advance(3, 400000, 299000)
				advance(4, 600000, 399000)
				So(mon.Metric(), ShouldEqual, 1) // (400000-299000)/100000 = 1
				advance(5, 800000, 499000)       // note: we're now where the producer was at +- t=3.5, so 1.5s behind
				So(mon.Metric(), ShouldEqual, 2) // (600000-399000)/100000 = 2
				advance(6, 1000000, 599000)      // note: we're now at where the producer was at +- t=4, so 2 seconds behind
				So(mon.Metric(), ShouldEqual, 3) // (800000-499000)/100000 = 3
			})
			Convey("a GC pause is causing us to not be able to consume during a few seconds", func() {
				advance(3, 300000, 199000)
				advance(4, 400000, 199000)
				// TODO: this punishes really hard for short GC pauses
				So(mon.Metric(), ShouldEqual, 101000) // ~(300000-199000)/0 -> 101000
				// TODO: test what happens during recovery
			})
		})
	})
	Convey("with lots of measurements", t, func() {
		now := time.Now()
		for part := range mon.monitors {
			for i := 0; i < 100; i++ {
				mon.StoreOffsets(part, int64(i), int64(2*i), now.Add(time.Second*time.Duration(i)))
			}
		}
		So(mon.Metric(), ShouldEqual, 90)
	})
	Convey("metric should be worst partition", t, func() {
		now := time.Now()
		for part := range mon.monitors {
			mon.StoreOffsets(part, int64(part), int64(2*part+10), now.Add(time.Second*time.Duration(part)))
		}
		So(mon.Metric(), ShouldEqual, 13)
	})
}
