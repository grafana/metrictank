package main

import (
	"testing"
)

type testcase struct {
	ts       uint32
	span     uint32
	boundary uint32
}

func TestAggBoundary(t *testing.T) {
	cases := []testcase{
		{1, 120, 120},
		{50, 120, 120},
		{118, 120, 120},
		{119, 120, 120},
		{120, 120, 120},
		{121, 120, 240},
		{122, 120, 240},
		{150, 120, 240},
		{237, 120, 240},
		{238, 120, 240},
		{239, 120, 240},
		{240, 120, 240},
		{241, 120, 360},
	}
	for _, c := range cases {
		if ret := aggBoundary(c.ts, c.span); ret != c.boundary {
			t.Fatalf("aggBoundary for ts %d with span %d should be %d, not %d", c.ts, c.span, c.boundary, ret)
		}
	}
}

// note that values don't get "committed" to the metric until the aggregation interval is complete
func TestAggregator(t *testing.T) {
	compare := func(key string, metric Metric, expected []Point) {
		_, iters := metric.Get(0, 1000)
		got := make([]Point, 0, len(expected))
		for _, iter := range iters {
			for iter.Next() {
				ts, val := iter.Values()
				got = append(got, Point{val, ts})
			}
		}
		if len(got) != len(expected) {
			t.Fatalf("output for testcase %s mismatch: expected: %v points, got: %v", key, len(expected), len(got))

		} else {
			for i, g := range got {
				exp := expected[i]
				if exp.Val != g.Val || exp.Ts != g.Ts {
					t.Fatalf("output for testcase %s mismatch at point %d: expected: %v, got: %v", key, i, exp, g)
				}
			}
		}
	}
	agg := NewAggregator("test", 60, 120, 10)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	expected := []Point{}
	compare("simple-min-unfinished", agg.minMetric, expected)

	agg = NewAggregator("test", 60, 120, 10)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(130, 130)
	expected = []Point{
		{5, 120},
	}
	compare("simple-min-one-block", agg.minMetric, expected)

	agg = NewAggregator("test", 60, 120, 10)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(120, 4)
	expected = []Point{
		{4, 120},
	}
	compare("simple-min-one-block-done-cause-last-point-just-right", agg.minMetric, expected)

}
