package schema

import (
	"math"
	"testing"
)

func TestMKeyConversionBothWays(t *testing.T) {

	cases := []struct {
		idStr   string
		expErr  bool
		expMKey MKey
	}{
		{".00112233445566778899aabbccddeeff", true, MKey{}},
		{"0.0112233445566778899aabbccddeeff", true, MKey{}},
		{"a.00112233445566778899aabbccddeeff", true, MKey{}},
		{"0.00112233445566778899aabbccddeeff", false, MKey{[16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}, 0}},
		{"1.00112233445566778899aabbccddeeff", false, MKey{[16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}, 1}},
		{"1234567.00112233445566778899aabbccddeeff", false, MKey{[16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}, 1234567}},
		{"4294967295.00112233445566778899aabbccddeeff", false, MKey{[16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}, math.MaxUint32}},
	}

	for i, c := range cases {
		mk, err := MKeyFromString(c.idStr)
		if (err != nil) != c.expErr {
			t.Fatalf("case %d exp err %v got %v", i, c.expErr, err)
		}
		if err != nil {
			continue
		}
		if mk != c.expMKey {
			t.Fatalf("case %d exp MKey %v got %v", i, c.expMKey, mk)
		}
		str := mk.String()
		if str != c.idStr {
			t.Fatalf("case %d exp MKey.String() %v got %v", i, c.idStr, str)
		}
	}
}

func TestAMKeyConversionBothWays(t *testing.T) {

	cases := []struct {
		idStr    string
		expErr   bool
		expAMKey AMKey
	}{
		{".00112233445566778899aabbccd_deeff", true, AMKey{}},
		{"0.0112233445566778899aab_bccd_deeff", true, AMKey{}},
		{"0.00112233445566778899aabbccddeeff", false, AMKey{MKey{[16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}, 0}, 0}},
		{"0.00112233445566778899aabbccddeeff_min_600", false, AMKey{MKey{[16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}, 0}, NewArchive(Min, 600)}},
	}

	for i, c := range cases {
		amk, err := AMKeyFromString(c.idStr)
		if (err != nil) != c.expErr {
			t.Fatalf("case %d exp err %v got %v", i, c.expErr, err)
		}
		if err != nil {
			continue
		}
		if amk != c.expAMKey {
			t.Fatalf("case %d exp MKey %v got %v", i, c.expAMKey, amk)
		}
		str := amk.String()
		if str != c.idStr {
			t.Fatalf("case %d exp MKey.String() %v got %v", i, c.idStr, str)
		}
	}
}
