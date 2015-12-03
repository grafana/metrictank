package main

import (
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"testing"
)

type testCase struct {
	in     []Point
	consol consolidation.Consolidator
	num    int
	out    []Point
}

func TestDataProcessor(t *testing.T) {
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
	}
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
