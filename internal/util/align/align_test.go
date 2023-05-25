package align

import "testing"

type pbCase struct {
	ts       uint32
	interval uint32
	expOut   uint32
}

func TestBackward(t *testing.T) {
	cases := []pbCase{
		{1, 60, 0},
		{2, 60, 0},
		{3, 60, 0},
		{57, 60, 0},
		{58, 60, 0},
		{59, 60, 0},
		{60, 60, 0},
		{61, 60, 60},
		{62, 60, 60},
		{63, 60, 60},
	}
	for _, c := range cases {
		if ret := Backward(c.ts, c.interval); ret != c.expOut {
			t.Fatalf("Backward for ts %d with interval %d should be %d, not %d", c.ts, c.interval, c.expOut, ret)
		}
	}
}

func TestBackwardIfNotAligned(t *testing.T) {
	cases := []pbCase{
		{1, 60, 0},
		{2, 60, 0},
		{3, 60, 0},
		{57, 60, 0},
		{58, 60, 0},
		{59, 60, 0},
		{60, 60, 60},
		{61, 60, 60},
		{62, 60, 60},
		{63, 60, 60},
	}
	for _, c := range cases {
		if ret := BackwardIfNotAligned(c.ts, c.interval); ret != c.expOut {
			t.Fatalf("BackwardIfNotAligned for ts %d with interval %d should be %d, not %d", c.ts, c.interval, c.expOut, ret)
		}
	}
}

func TestForward(t *testing.T) {
	cases := []pbCase{
		{1, 60, 60},
		{2, 60, 60},
		{3, 60, 60},
		{57, 60, 60},
		{58, 60, 60},
		{59, 60, 60},
		{60, 60, 120},
		{61, 60, 120},
		{62, 60, 120},
		{63, 60, 120},
	}
	for _, c := range cases {
		if ret := Forward(c.ts, c.interval); ret != c.expOut {
			t.Fatalf("Forward for ts %d with interval %d should be %d, not %d", c.ts, c.interval, c.expOut, ret)
		}
	}
}

func TestForwardIfNotAligned(t *testing.T) {
	cases := []pbCase{
		{1, 60, 60},
		{2, 60, 60},
		{3, 60, 60},
		{57, 60, 60},
		{58, 60, 60},
		{59, 60, 60},
		{60, 60, 60},
		{61, 60, 120},
		{62, 60, 120},
		{63, 60, 120},
	}
	for _, c := range cases {
		if ret := ForwardIfNotAligned(c.ts, c.interval); ret != c.expOut {
			t.Fatalf("ForwardIfNotAligned for ts %d with interval %d should be %d, not %d", c.ts, c.interval, c.expOut, ret)
		}
	}
}
