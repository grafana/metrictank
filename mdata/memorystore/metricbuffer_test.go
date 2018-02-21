package memorystore

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/raintank/schema.v1"
)

func TestGetInterval(t *testing.T) {
	Convey("with valid interval between points", t, func() {
		points := []schema.Point{
			{
				Ts:  10,
				Val: 0,
			},
			{
				Ts:  20,
				Val: 0,
			},
			{
				Ts:  30,
				Val: 0,
			},
		}
		interval := getInterval(points)
		So(interval, ShouldEqual, 10)
	})
	Convey("with intervals off by 10%", t, func() {
		points := []schema.Point{
			{
				Ts:  11,
				Val: 0,
			},
			{
				Ts:  20,
				Val: 0,
			},
			{
				Ts:  31,
				Val: 0,
			},
		}
		interval := getInterval(points)
		So(interval, ShouldEqual, 10)
	})
	Convey("with points missing", t, func() {
		points := []schema.Point{
			{
				Ts:  10,
				Val: 0,
			},
			{
				Ts:  30,
				Val: 0,
			},
			{
				Ts:  40,
				Val: 0,
			},
			{
				Ts:  50,
				Val: 0,
			},
		}
		interval := getInterval(points)
		So(interval, ShouldEqual, 10)
	})
	Convey("with non standard interval", t, func() {
		points := []schema.Point{
			{
				Ts:  0,
				Val: 0,
			},
			{
				Ts:  35,
				Val: 0,
			},
			{
				Ts:  70,
				Val: 0,
			},
		}
		interval := getInterval(points)
		So(interval, ShouldEqual, 35)
	})
	Convey("with interval off by more then 10% - no match", t, func() {
		points := []schema.Point{
			{
				Ts:  0,
				Val: 0,
			},
			{
				Ts:  8,
				Val: 0,
			},
			{
				Ts:  19,
				Val: 0,
			},
		}
		interval := getInterval(points)
		So(interval, ShouldEqual, 0)
	})
	Convey("with interval off by more then 10% - match", t, func() {
		points := []schema.Point{
			{
				Ts:  0,
				Val: 0,
			},
			{
				Ts:  8,
				Val: 0,
			},
			{
				Ts:  19,
				Val: 0,
			},
			{
				Ts:  30,
				Val: 0,
			},
		}
		interval := getInterval(points)
		So(interval, ShouldEqual, 10)
	})
	Convey("with interval > 1h", t, func() {
		points := []schema.Point{
			{
				Ts:  0,
				Val: 0,
			},
			{
				Ts:  3700,
				Val: 0,
			},
			{
				Ts:  7500,
				Val: 0,
			},
		}
		interval := getInterval(points)
		So(interval, ShouldEqual, 3600)
	})
	Convey("with interval > 1d", t, func() {
		points := []schema.Point{
			{
				Ts:  0,
				Val: 0,
			},
			{
				Ts:  90000,
				Val: 0,
			},
			{
				Ts:  180000,
				Val: 0,
			},
		}
		interval := getInterval(points)
		So(interval, ShouldEqual, 86400)
	})
}
