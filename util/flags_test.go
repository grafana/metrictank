package util

import (
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestInt64SliceFlag(t *testing.T) {
	Convey("When setting Int64SliceFlag to empty string", t, func(c C) {
		intSliceFlag := Int64SliceFlag{}
		err := intSliceFlag.Set("")
		c.So(err, ShouldBeNil)
		c.So(intSliceFlag, ShouldHaveLength, 0)
	})
	Convey("When setting Int64SliceFlag has no values", t, func(c C) {
		intSliceFlag := Int64SliceFlag{}
		err := intSliceFlag.Set(", ")
		c.So(err, ShouldBeNil)
		c.So(intSliceFlag, ShouldHaveLength, 0)
	})

	Convey("When setting Int64SliceFlag to invalid value", t, func(c C) {
		intSliceFlag := Int64SliceFlag{}
		err := intSliceFlag.Set("foo")
		c.So(err, ShouldHaveSameTypeAs, &strconv.NumError{})
	})

	Convey("When setting Int64SliceFlag to single org", t, func(c C) {
		intSliceFlag := Int64SliceFlag{}
		err := intSliceFlag.Set("10")
		c.So(err, ShouldBeNil)
		c.So(intSliceFlag, ShouldHaveLength, 1)
		c.So(intSliceFlag[0], ShouldEqual, 10)
	})

	Convey("When setting Int64SliceFlag to many orgs", t, func(c C) {
		intSliceFlag := Int64SliceFlag{}
		err := intSliceFlag.Set("10,1,17")
		c.So(err, ShouldBeNil)
		c.So(intSliceFlag, ShouldHaveLength, 3)
		c.So(intSliceFlag[0], ShouldEqual, 10)
		c.So(intSliceFlag[1], ShouldEqual, 1)
		c.So(intSliceFlag[2], ShouldEqual, 17)
	})

	Convey("When Int64SliceFlag setting has spaces", t, func(c C) {
		intSliceFlag := Int64SliceFlag{}
		err := intSliceFlag.Set(" 10 , 1, 17")
		c.So(err, ShouldBeNil)
		c.So(intSliceFlag, ShouldHaveLength, 3)
		c.So(intSliceFlag[0], ShouldEqual, 10)
		c.So(intSliceFlag[1], ShouldEqual, 1)
		c.So(intSliceFlag[2], ShouldEqual, 17)
	})

	Convey("When Int64SliceFlag setting has repeated commas", t, func(c C) {
		intSliceFlag := Int64SliceFlag{}
		err := intSliceFlag.Set(",,,10")
		c.So(err, ShouldBeNil)
		c.So(intSliceFlag, ShouldHaveLength, 1)
	})
}
