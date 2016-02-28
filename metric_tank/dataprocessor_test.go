package main

import (
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"math"
	"testing"
)

type testCase struct {
	in     []Point
	consol consolidation.Consolidator
	num    uint32
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
	every         uint32
}

func TestAggEvery(t *testing.T) {
	cases := []c{
		{0, 1, 1},
		{1, 1, 1},
		{1, 2, 1},
		{2, 2, 1},
		{3, 2, 2},
		{4, 2, 2},
		{5, 2, 3},
		{0, 80, 1},
		{1, 80, 1},
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

func nullPoints(from, to, interval uint32) []Point {
	out := make([]Point, 0)
	for i := from; i < to; i += interval {
		out = append(out, Point{math.NaN(), i})
	}
	return out
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
		{
			// no data at all. saw this one for real
			[]Point{},
			1450242982,
			1450329382,
			600,
			nullPoints(1450243200, 1450329382, 600),
		},
		{
			// don't trip over last.
			[]Point{{1, 10}, {2, 20}, {2, 19}},
			10,
			31,
			10,
			[]Point{{1, 10}, {2, 20}, {math.NaN(), 30}},
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

type alignCase struct {
	reqs        []Req
	aggSettings []aggSetting
	outReqs     []Req
	outErr      error
}

func reqRaw(key string, from, to, maxPoints uint32, consolidator consolidation.Consolidator, rawInterval uint32) Req {
	req := NewReq(key, key, from, to, maxPoints, consolidator)
	req.rawInterval = rawInterval
	return req
}
func reqOut(key string, from, to, maxPoints uint32, consolidator consolidation.Consolidator, rawInterval uint32, archive int, archInterval, outInterval, aggNum uint32) Req {
	req := NewReq(key, key, from, to, maxPoints, consolidator)
	req.rawInterval = rawInterval
	req.archive = archive
	req.archInterval = archInterval
	req.outInterval = outInterval
	req.aggNum = aggNum
	return req
}

func TestAlignRequests(t *testing.T) {
	input := []alignCase{
		{
			// real example seen with alerting queries
			[]Req{
				reqRaw("a", 0, 30, 800, consolidation.Avg, 10),
				reqRaw("b", 0, 30, 800, consolidation.Avg, 60),
			},
			[]aggSetting{},
			[]Req{
				reqOut("a", 0, 30, 800, consolidation.Avg, 10, 0, 10, 60, 6),
				reqOut("b", 0, 30, 800, consolidation.Avg, 60, 0, 60, 60, 1),
			},
			nil,
		},
	}
	for i, ac := range input {
		out, err := alignRequests(ac.reqs, ac.aggSettings)
		if err != ac.outErr {
			t.Errorf("different err value for testcase %d  expected: %v, got: %v", i, ac.outErr, err)
		}
		if len(out) != len(ac.outReqs) {
			t.Errorf("different amount of requests for testcase %d  expected: %v, got: %v", i, len(ac.outReqs), len(out))
		} else {
			for r, exp := range ac.outReqs {
				if exp != out[r] {
					t.Errorf("testcase %d, request %d:\nexpected: %v\n     got: %v", i, r, exp.DebugString(), out[r].DebugString())
				}
			}
		}
	}
}

var result []Req

func BenchmarkAlignRequests(b *testing.B) {
	var res []Req
	reqs := []Req{
		reqRaw("a", 0, 3600*24*7, 1000, consolidation.Avg, 10),
		reqRaw("b", 0, 3600*24*7, 1000, consolidation.Avg, 30),
		reqRaw("c", 0, 3600*24*7, 1000, consolidation.Avg, 60),
	}
	aggSettings := []aggSetting{
		{600, 21600, 1, 0, true},
		{7200, 21600, 1, 0, true},
		{21600, 21600, 1, 0, true},
	}

	for n := 0; n < b.N; n++ {
		res, _ = alignRequests(reqs, aggSettings)
	}
	result = res
}
