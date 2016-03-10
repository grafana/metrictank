package dur

import "testing"

func TestUsec(t *testing.T) {
	var cases = []struct {
		in  string
		out uint32
		err bool
	}{
		{"", 0, true},
		{"0", 0, false},
		{"-1", 0, true},
		{"1", 1, false},
		{"3600", 3600, false},
		{"1000000000", 1000000000, false},
		{"1us", 0, true},
		{"1ms", 0, true},
		{"1000ms", 0, true},
		{"1m", 60, false},
		{"1min", 60, false},
		{"1h", 3600, false},
		{"1s", 1, false},
		{"2d", 2 * 60 * 60 * 24, false},
		{"10hours", 60 * 60 * 10, false},
		{"7d13h45min21s", 7*24*60*60 + 13*60*60 + 45*60 + 21, false},
		{"01hours", 60 * 60 * 1, false},
		{"2d2d", 4 * 60 * 60 * 24, false},
	}

	for i, c := range cases {
		d, err := ParseUsec(c.in)
		if (err != nil) != c.err {
			t.Fatalf("case %d %q: expected err %t, got err %s", i, c.in, c.err, err)
		}
		if d != c.out {
			t.Fatalf("case %d %q: expected %d, got %d", i, c.in, c.out, d)
		}
	}
}
