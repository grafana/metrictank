package main

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
	for _, c := range cases {
		out := lcm(c.in)
		if out != c.out {
			t.Errorf("%s -> expected %d, got %d", c.out, out)
		}
	}
}
