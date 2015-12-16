package main

import (
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"math"
	"testing"
)

type testCase struct {
	in     []Point
	consol consolidation.Consolidator
	num    int
	out    []Point
}

func validate(cases []testCase, t *testing.T) {
	for i, c := range cases {
		out := consolidate(c.in, c.num, c.consol)
		if len(out) != len(c.out) {
			t.Fatalf("output for testcase %d mismatch: expected: %v, got: %v", i, c.out, out)

		} else {
			for j, p := range out {
				if p.Val != c.out[j].Val || p.Ts != c.out[j].Ts {
					t.Fatalf("output for testcase %d mismatch at point %d: expected: %v, got: %v", i, j, c.out[j], out[j])
				}
			}
		}
	}
}

func TestOddConsolidationAlignments(t *testing.T) {
	cases := []testCase{
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Avg,
			1,
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
		},
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Avg,
			3,
			[]Point{
				{2, 1449178151},
				{4, 1449178181}, // see comment below
			},
		},
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
			},
			consolidation.Avg,
			1,
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
			},
		},
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
			},
			consolidation.Avg,
			2,
			[]Point{
				{1.5, 1449178141},
				{3, 1449178161}, // note: we choose the next ts here for even spacing (important for further processing/parsing/handing off), even though that point is missing
			},
		},
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
			},
			consolidation.Avg,
			3,
			[]Point{
				{2, 1449178151},
			},
		},
	}
	validate(cases, t)
}
func TestConsolidationFunctions(t *testing.T) {
	cases := []testCase{
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Avg,
			2,
			[]Point{
				{1.5, 1449178141},
				{3.5, 1449178161},
			},
		},
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Cnt,
			2,
			[]Point{
				{2, 1449178141},
				{2, 1449178161},
			},
		},
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Last,
			2,
			[]Point{
				{2, 1449178141},
				{4, 1449178161},
			},
		},
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Min,
			2,
			[]Point{
				{1, 1449178141},
				{3, 1449178161},
			},
		},
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Max,
			2,
			[]Point{
				{2, 1449178141},
				{4, 1449178161},
			},
		},
		{
			[]Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Sum,
			2,
			[]Point{
				{3, 1449178141},
				{7, 1449178161},
			},
		},
	}
	validate(cases, t)
}

type c struct {
	numPoints     uint32
	maxDataPoints uint32
	every         int
}

func TestAggEvery(t *testing.T) {
	cases := []c{
		{60, 80, 1},
		{70, 80, 1},
		{79, 80, 1},
		{80, 80, 1},
		{81, 80, 2},
		{120, 80, 2},
		{150, 80, 2},
		{158, 80, 2},
		{159, 80, 2},
		{160, 80, 2},
		{161, 80, 3},
		{165, 80, 3},
		{180, 80, 3},
	}
	for i, c := range cases {
		every := aggEvery(c.numPoints, c.maxDataPoints)
		if every != c.every {
			t.Fatalf("output for testcase %d mismatch: expected: %v, got: %v", i, c.every, every)
		}
	}
}

type fixc struct {
	in       []Point
	from     uint32
	to       uint32
	interval uint32
	out      []Point
}

func TestFix(t *testing.T) {
	cases := []fixc{
		{
			// the most standard simple case
			[]Point{{1, 10}, {2, 20}, {3, 30}},
			10,
			31,
			10,
			[]Point{{1, 10}, {2, 20}, {3, 30}},
		},
		{
			// almost... need Nan in front
			[]Point{{1, 10}, {2, 20}, {3, 30}},
			1,
			31,
			10,
			[]Point{{1, 10}, {2, 20}, {3, 30}},
		},
		{
			// need Nan in front
			[]Point{{1, 10}, {2, 20}, {3, 30}},
			0,
			31,
			10,
			[]Point{{math.NaN(), 0}, {1, 10}, {2, 20}, {3, 30}},
		},
		{
			// almost..need Nan in back
			[]Point{{1, 10}, {2, 20}, {3, 30}},
			10,
			40,
			10,
			[]Point{{1, 10}, {2, 20}, {3, 30}},
		},
		{
			// need Nan in back
			[]Point{{1, 10}, {2, 20}, {3, 30}},
			10,
			41,
			10,
			[]Point{{1, 10}, {2, 20}, {3, 30}, {math.NaN(), 40}},
		},
		{
			// need Nan in middle
			[]Point{{1, 10}, {3, 30}},
			10,
			31,
			10,
			[]Point{{1, 10}, {math.NaN(), 20}, {3, 30}},
		},
		{
			// need Nan everywhere
			[]Point{{2, 20}, {4, 40}, {7, 70}},
			0,
			90,
			10,
			[]Point{{math.NaN(), 0}, {math.NaN(), 10}, {2, 20}, {math.NaN(), 30}, {4, 40}, {math.NaN(), 50}, {math.NaN(), 60}, {7, 70}, {math.NaN(), 80}},
		},
		{
			// too much data. note that there are multiple satisfactory solutions here. this is just one of them.
			[]Point{{10, 10}, {14, 14}, {20, 20}, {26, 26}, {35, 35}},
			10,
			41,
			10,
			[]Point{{10, 10}, {14, 20}, {26, 30}, {35, 40}},
		},
	}
	for i, c := range cases {
		got := fix(c.in, c.from, c.to, c.interval)

		if len(c.out) != len(got) {
			t.Fatalf("output for testcase %d mismatch: expected: %v, got: %v", i, c.out, got)
		}
		for j, pgot := range got {
			pexp := c.out[j]
			gotNan := math.IsNaN(pgot.Val)
			expNan := math.IsNaN(pexp.Val)
			if gotNan != expNan || (!gotNan && pgot.Val != pexp.Val) || pgot.Ts != pexp.Ts {
				t.Fatalf("output for testcase %d at point %d mismatch: expected: %v, got: %v", i, j, c.out, got)
			}
		}
	}

}
