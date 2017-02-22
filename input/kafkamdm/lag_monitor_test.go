package kafkamdm

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLagLogger(t *testing.T) {
	logger := newLagLogger(5)
	Convey("with 0 measurements", t, func() {
		So(logger.Min(), ShouldEqual, 0)
	})
	Convey("with 1 measurements", t, func() {
		logger.Store(10)
		So(logger.Min(), ShouldEqual, 10)
	})
	Convey("with 2 measurements", t, func() {
		logger.Store(5)
		So(logger.Min(), ShouldEqual, 5)
	})
	Convey("with lots of measurements", t, func() {
		for i := 0; i < 100; i++ {
			logger.Store(i)
		}
		So(logger.Min(), ShouldEqual, 95)
	})
}

func TestLagMonitor(t *testing.T) {
	mon := NewLagMonitor(10, []int32{0, 1, 2, 3})
	Convey("with 0 measurements", t, func() {
		So(mon.Metric(), ShouldEqual, 0)
	})
	Convey("with lots of measurements", t, func() {
		for part := range mon.lag {
			for i := 0; i < 100; i++ {
				mon.Store(part, i)
			}
		}
		So(mon.Metric(), ShouldEqual, 90)
	})
	Convey("metric should be worst partition", t, func() {
		for part := range mon.lag {
			mon.Store(part, 10+int(part))
		}
		So(mon.Metric(), ShouldEqual, 13)
	})
}
