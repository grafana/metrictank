package conf

import "testing"

func TestNumSeries(t *testing.T) {
	type testCase struct {
		methods   []Method
		numSeries int
	}
	cases := []testCase{
		{
			[]Method{Sum},
			1,
		},
		{
			[]Method{Avg},
			2,
		},
		{
			[]Method{Sum, Avg},
			2,
		},
		{
			[]Method{Lst, Max, Min},
			3,
		},
		{
			[]Method{Avg, Lst, Max, Min},
			5,
		},
	}
	for i, c := range cases {
		n := NumSeries(c.methods)
		if n != c.numSeries {
			t.Errorf("case %d: expected %d, got %d", i, c.numSeries, n)
		}
	}
}
