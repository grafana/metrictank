package util

import (
	"testing"
)

func TestLCM(t *testing.T) {
	cases := []struct {
		in  []uint32
		out uint32
	}{
		{[]uint32{10, 60}, 60},
		{[]uint32{20, 30}, 60},
		{[]uint32{40, 60}, 120},
		{[]uint32{1, 3}, 3},
	}
	for i, c := range cases {
		out := Lcm(c.in)
		if out != c.out {
			t.Errorf("case %d -> expected %d, got %d", i, c.out, out)
		}
	}
}
