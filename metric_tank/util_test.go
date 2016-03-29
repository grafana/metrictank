package main

import (
	"testing"
)

func TestLCM(t *testing.T) {
	cases := []struct {
		in  []uint16
		out uint16
	}{
		{[]uint16{10, 60}, 60},
		{[]uint16{20, 30}, 60},
		{[]uint16{40, 60}, 120},
		{[]uint16{1, 3}, 3},
	}
	for i, c := range cases {
		out := lcm(c.in)
		if out != c.out {
			t.Errorf("case %d -> expected %d, got %d", i, c.out, out)
		}
	}
}
