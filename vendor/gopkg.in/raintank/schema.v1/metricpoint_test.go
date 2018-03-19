package schema

import (
	"math"
	"reflect"
	"testing"
)

func TestMetricPointMarshal(t *testing.T) {
	tests := []MetricPoint{
		{
			MKey:  MKey{[16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}, 12345},
			Time:  0,
			Value: 0,
		},
		{
			MKey:  MKey{[16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, math.MaxUint32},
			Time:  1234567890,
			Value: 123.456789,
		},
		{
			MKey:  MKey{[16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}, 0},
			Time:  math.MaxUint32,
			Value: math.MaxFloat64,
		},
	}
	for i, in := range tests {
		// marshal with nil input
		data1, err := in.Marshal(nil)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if len(data1) != 32 {
			t.Fatalf("case %d did not result in 32B data packet", i)
		}

		// marshal with sufficient input
		buf2 := make([]byte, 0, 32)
		data2, err := in.Marshal(buf2)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data2[:32]) {
			t.Fatalf("case %d marshaling mismatch.", i)
		}

		// marshal with very big input
		buf3 := make([]byte, 0, 512)
		data3, err := in.Marshal(buf3)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data3[:32]) {
			t.Fatalf("case %d marshaling mismatch", i)
		}

		// Marshal32 with sufficient input
		buf4 := make([]byte, 0, 32)
		data4, err := in.Marshal32(buf4)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data4[:32]) {
			t.Fatalf("case %d marshaling mismatch.", i)
		}

		// Marshal32 with very big input
		buf5 := make([]byte, 0, 512)
		data5, err := in.Marshal32(buf5)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data5[:32]) {
			t.Fatalf("case %d marshaling mismatch", i)
		}

		// pre-existing input that should be left alone
		buf6 := []byte{'f', 'o', 'o'}
		data6, err := in.Marshal(buf6)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data6[3:]) {
			t.Fatalf("case %d marshaling mismatch", i)
		}
		if string(data6[0:3]) != "foo" {
			t.Fatalf("case %d pre-existing data was modified to %q", i, string(data6[0:3]))
		}

		// unmarshal

		out := MetricPoint{}
		leftover, err := out.Unmarshal(data1)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if len(leftover) != 0 {
			t.Fatalf("case %d got leftover data: %v", i, leftover)
		}
		if in.MKey != out.MKey || in.Time != out.Time || in.Value != out.Value {
			t.Fatalf("case %d data mismatch:\nexp: %v\ngot: %v", i, in, out)
		}
	}
}

func TestMetricPointMarshalMultiple(t *testing.T) {
	tests := []MetricPoint{
		{
			MKey:  MKey{[16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}, math.MaxUint32},
			Time:  0,
			Value: 0,
		},
		{
			MKey:  MKey{[16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}, 0},
			Time:  math.MaxUint32,
			Value: math.MaxFloat64,
		},
		{
			MKey:  MKey{[16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}, math.MaxUint32},
			Time:  0,
			Value: 0,
		},
		{
			MKey:  MKey{[16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}, 0},
			Time:  math.MaxUint32,
			Value: math.MaxFloat64,
		},
	}
	var buf []byte
	for i, in := range tests {
		// marshal with nil input
		var err error
		buf, err = in.Marshal(buf)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		expLen := (i + 1) * 32
		if len(buf) != expLen {
			t.Fatalf("iteration %d . expected length %d, got %d", i, expLen, len(buf))
		}
	}
	type byteCheck struct {
		pos     int
		val     uint8
		comment string
	}
	checks := []byteCheck{
		{0, 0, "byte 0 of id of first"},
		{12, 12, "byte 12 of id of first"},
		{27, 0, "last byte of value of first"},
		{31, 255, "first byte of org of first"},
		{32*2 + 0, 0, "byte 0 of id of third"},
		{32*2 + 12, 12, "byte 12 of id of third"},
		{32*2 + 27, 0, "last byte of value of third"},
		{32*2 + 31, 255, "first byte of org of third"},

		{32 + 0, 255, "byte 0 of id of second"},
		{32 + 12, 255, "byte 12 of id of second"},
		{32 + 27, 255, "last byte of value of second"},
		{32 + 31, 0, "first byte of org of second"},
		{32*3 + 0, 255, "byte 0 of id of fourth"},
		{32*3 + 12, 255, "byte 12 of id of fourth"},
		{32*3 + 27, 255, "last byte of value of fourth"},
		{32*3 + 31, 0, "first byte of org of fourth"},
	}
	for _, check := range checks {
		if buf[check.pos] != check.val {
			t.Fatalf("%s: expected val %d, got %d", check.comment, check.val, buf[check.pos])
		}
	}
}
