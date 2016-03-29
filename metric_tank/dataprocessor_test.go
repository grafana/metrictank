package main

import (
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"github.com/raintank/raintank-metric/schema"
	"math"
	"math/rand"
	"testing"
)

type testCase struct {
	in     []schema.Point
	consol consolidation.Consolidator
	num    uint32
	out    []schema.Point
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
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Avg,
			1,
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
		},
		{
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Avg,
			3,
			[]schema.Point{
				{2, 1449178151},
				{4, 1449178181}, // see comment below
			},
		},
		{
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
			},
			consolidation.Avg,
			1,
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
			},
		},
		{
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
			},
			consolidation.Avg,
			2,
			[]schema.Point{
				{1.5, 1449178141},
				{3, 1449178161}, // note: we choose the next ts here for even spacing (important for further processing/parsing/handing off), even though that point is missing
			},
		},
		{
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
			},
			consolidation.Avg,
			3,
			[]schema.Point{
				{2, 1449178151},
			},
		},
	}
	validate(cases, t)
}
func TestConsolidationFunctions(t *testing.T) {
	cases := []testCase{
		{
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Avg,
			2,
			[]schema.Point{
				{1.5, 1449178141},
				{3.5, 1449178161},
			},
		},
		{
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Cnt,
			2,
			[]schema.Point{
				{2, 1449178141},
				{2, 1449178161},
			},
		},
		{
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Min,
			2,
			[]schema.Point{
				{1, 1449178141},
				{3, 1449178161},
			},
		},
		{
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Max,
			2,
			[]schema.Point{
				{2, 1449178141},
				{4, 1449178161},
			},
		},
		{
			[]schema.Point{
				{1, 1449178131},
				{2, 1449178141},
				{3, 1449178151},
				{4, 1449178161},
			},
			consolidation.Sum,
			2,
			[]schema.Point{
				{3, 1449178141},
				{7, 1449178161},
			},
		},
	}
	validate(cases, t)
}

type c struct {
	numPoints     uint32
	maxDataPoints uint16
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

func TestDivide(t *testing.T) {
	cases := []struct {
		a   []schema.Point
		b   []schema.Point
		out []schema.Point
	}{
		{
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}},
			[]schema.Point{{2, 10}, {2, 20}, {1, 30}},
			[]schema.Point{{0.5, 10}, {1, 20}, {3, 30}},
		},
		{
			[]schema.Point{{100, 10}, {5000, 20}, {150.5, 30}, {150.5, 40}},
			[]schema.Point{{2, 10}, {0.5, 20}, {2, 30}, {0.5, 40}},
			[]schema.Point{{50, 10}, {10000, 20}, {75.25, 30}, {301, 40}},
		},
	}
	for i, c := range cases {
		got := divide(c.a, c.b)

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

type fixc struct {
	in       []schema.Point
	from     uint32
	to       uint32
	interval uint16
	out      []schema.Point
}

func nullPoints(from, to uint32, interval uint16) []schema.Point {
	out := make([]schema.Point, 0)
	for i := from; i < to; i += uint32(interval) {
		out = append(out, schema.Point{math.NaN(), i})
	}
	return out
}

func TestFix(t *testing.T) {
	cases := []fixc{
		{
			// the most standard simple case
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}},
			10,
			31,
			10,
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}},
		},
		{
			// almost... need Nan in front
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}},
			1,
			31,
			10,
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}},
		},
		{
			// need Nan in front
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}},
			0,
			31,
			10,
			[]schema.Point{{math.NaN(), 0}, {1, 10}, {2, 20}, {3, 30}},
		},
		{
			// almost..need Nan in back
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}},
			10,
			40,
			10,
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}},
		},
		{
			// need Nan in back
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}},
			10,
			41,
			10,
			[]schema.Point{{1, 10}, {2, 20}, {3, 30}, {math.NaN(), 40}},
		},
		{
			// need Nan in middle
			[]schema.Point{{1, 10}, {3, 30}},
			10,
			31,
			10,
			[]schema.Point{{1, 10}, {math.NaN(), 20}, {3, 30}},
		},
		{
			// need Nan everywhere
			[]schema.Point{{2, 20}, {4, 40}, {7, 70}},
			0,
			90,
			10,
			[]schema.Point{{math.NaN(), 0}, {math.NaN(), 10}, {2, 20}, {math.NaN(), 30}, {4, 40}, {math.NaN(), 50}, {math.NaN(), 60}, {7, 70}, {math.NaN(), 80}},
		},
		{
			// too much data. note that there are multiple satisfactory solutions here. this is just one of them.
			[]schema.Point{{10, 10}, {14, 14}, {20, 20}, {26, 26}, {35, 35}},
			10,
			41,
			10,
			[]schema.Point{{10, 10}, {14, 20}, {26, 30}, {35, 40}},
		},
		{
			// no data at all. saw this one for real
			[]schema.Point{},
			1450242982,
			1450329382,
			600,
			nullPoints(1450243200, 1450329382, 600),
		},
		{
			// don't trip over last.
			[]schema.Point{{1, 10}, {2, 20}, {2, 19}},
			10,
			31,
			10,
			[]schema.Point{{1, 10}, {2, 20}, {math.NaN(), 30}},
		},
		{
			// interval > from-to span with empty input.  saw this one in prod
			// 1458966240 divides by 60 but is too low
			// 1458966300 divides by 60 but is too high
			// so there is no point in range, so output must be empty
			[]schema.Point{},
			1458966244, // 1458966240 divides by 60 but is too low
			1458966274, // 1458966300 divides by 60 but is too high
			60,
			[]schema.Point{},
		},
		{
			// let's try a similar case but now there is a point in range
			[]schema.Point{},
			1458966234,
			1458966264,
			60,
			[]schema.Point{{math.NaN(), 1458966240}},
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

func reqRaw(key string, from, to uint32, maxPoints uint16, consolidator consolidation.Consolidator, rawInterval uint16) Req {
	req := NewReq(key, key, from, to, maxPoints, consolidator)
	req.rawInterval = rawInterval
	return req
}
func reqOut(key string, from, to uint32, maxPoints uint16, consolidator consolidation.Consolidator, rawInterval uint16, archive uint8, archInterval, outInterval uint16, aggNum uint32) Req {
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
		{
			// raw would be 3600/10=360 points, agg1 3600/60=60. raw is best cause it provides most points
			// and still under the max points limit.
			[]Req{
				reqRaw("a", 0, 3600, 800, consolidation.Avg, 10),
				reqRaw("b", 0, 3600, 800, consolidation.Avg, 10),
				reqRaw("c", 0, 3600, 800, consolidation.Avg, 10),
			},
			// span, chunkspan, numchunks, ttl, ready
			[]aggSetting{
				{60, 600, 2, 0, true},
				{120, 600, 1, 0, true},
			},
			[]Req{
				reqOut("a", 0, 3600, 800, consolidation.Avg, 10, 0, 10, 10, 1),
				reqOut("b", 0, 3600, 800, consolidation.Avg, 10, 0, 10, 10, 1),
				reqOut("c", 0, 3600, 800, consolidation.Avg, 10, 0, 10, 10, 1),
			},
			nil,
		},
		{
			// same as before, but agg1 disabled. just to make sure it still behaves the same.
			[]Req{
				reqRaw("a", 0, 3600, 800, consolidation.Avg, 10),
				reqRaw("b", 0, 3600, 800, consolidation.Avg, 10),
				reqRaw("c", 0, 3600, 800, consolidation.Avg, 10),
			},
			// span, chunkspan, numchunks, ttl, ready
			[]aggSetting{
				{60, 600, 2, 0, false},
				{120, 600, 1, 0, true},
			},
			[]Req{
				reqOut("a", 0, 3600, 800, consolidation.Avg, 10, 0, 10, 10, 1),
				reqOut("b", 0, 3600, 800, consolidation.Avg, 10, 0, 10, 10, 1),
				reqOut("c", 0, 3600, 800, consolidation.Avg, 10, 0, 10, 10, 1),
			},
			nil,
		},
		{
			// now we request 0-2400, with max datapoints 100.
			// raw: 2400/10 -> 240pts -> needs runtime consolidation
			// agg1: 2400/60 -> 40 pts, good candidate,
			// though 40pts is 2.5x smaller than the maxPoints target (100 points)
			// raw's 240 pts is only 2.4x larger then 100pts, so it is selected.
			[]Req{
				reqRaw("a", 0, 2400, 100, consolidation.Avg, 10),
				reqRaw("b", 0, 2400, 100, consolidation.Avg, 10),
				reqRaw("c", 0, 2400, 100, consolidation.Avg, 10),
			},
			[]aggSetting{
				{60, 600, 2, 0, true},
				{120, 600, 1, 0, true},
			},
			[]Req{
				reqOut("a", 0, 2400, 100, consolidation.Avg, 10, 0, 10, 30, 3),
				reqOut("b", 0, 2400, 100, consolidation.Avg, 10, 0, 10, 30, 3),
				reqOut("c", 0, 2400, 100, consolidation.Avg, 10, 0, 10, 30, 3),
			},
			nil,
		},
		// same thing as above, but now we set max points to 39. So now the 240pts
		// raw: 2400/10 -> 240 pts is 6.15x our target of 39pts
		// agg1 2400/120 -> 20 pts is only 1.95x smaller then our target 39pts so it is
		// selected.
		{
			[]Req{
				reqRaw("a", 0, 2400, 39, consolidation.Avg, 10),
				reqRaw("b", 0, 2400, 39, consolidation.Avg, 10),
				reqRaw("c", 0, 2400, 39, consolidation.Avg, 10),
			},
			[]aggSetting{
				{120, 600, 2, 0, true},
				{600, 600, 2, 0, true},
			},
			[]Req{
				reqOut("a", 0, 2400, 39, consolidation.Avg, 10, 1, 120, 120, 1),
				reqOut("b", 0, 2400, 39, consolidation.Avg, 10, 1, 120, 120, 1),
				reqOut("c", 0, 2400, 39, consolidation.Avg, 10, 1, 120, 120, 1),
			},
			nil,
		},
		// same as above, except now the 120s band is disabled.
		// raw: 2400/10 -> 240 pts is 6.15x our target of 39pts
		// agg1 2400/600 -> 4 pts is about 10x smaller so we prefer raw again
		{
			[]Req{
				reqRaw("a", 0, 2400, 39, consolidation.Avg, 10),
				reqRaw("b", 0, 2400, 39, consolidation.Avg, 10),
				reqRaw("c", 0, 2400, 39, consolidation.Avg, 10),
			},
			[]aggSetting{
				{120, 600, 2, 0, false},
				{600, 600, 2, 0, true},
			},
			// rawInterval, archive, archInterval, outInterval, aggNum
			[]Req{
				reqOut("a", 0, 2400, 39, consolidation.Avg, 10, 0, 10, 70, 7),
				reqOut("b", 0, 2400, 39, consolidation.Avg, 10, 0, 10, 70, 7),
				reqOut("c", 0, 2400, 39, consolidation.Avg, 10, 0, 10, 70, 7),
			},
			nil,
		},
		// same as above, 120s band is still disabled. but we query for a longer range
		// raw: 24000/10 -> 2400 pts is 61.5x our target of 39pts
		// agg2 24000/600 -> 40 pts is just too large, but we can make it work with runtime
		// consolidation
		{
			[]Req{
				reqRaw("a", 0, 24000, 39, consolidation.Avg, 10),
				reqRaw("b", 0, 24000, 39, consolidation.Avg, 10),
				reqRaw("c", 0, 24000, 39, consolidation.Avg, 10),
			},
			[]aggSetting{
				{120, 600, 2, 0, false},
				{600, 600, 2, 0, true},
			},
			// rawInterval, archive, archInterval, outInterval, aggNum
			[]Req{
				reqOut("a", 0, 24000, 39, consolidation.Avg, 10, 2, 600, 1200, 2),
				reqOut("b", 0, 24000, 39, consolidation.Avg, 10, 2, 600, 1200, 2),
				reqOut("c", 0, 24000, 39, consolidation.Avg, 10, 2, 600, 1200, 2),
			},
			nil,
		},
		// now something a bit different. 3 different raw intervals, but same aggregation settings.
		// raw is here best again but all series need to be at a step of 60
		// so runtime consolidation is needed, we'll get 40 points for each metric
		{
			[]Req{
				reqRaw("a", 0, 2400, 100, consolidation.Avg, 10),
				reqRaw("b", 0, 2400, 100, consolidation.Avg, 30),
				reqRaw("c", 0, 2400, 100, consolidation.Avg, 60),
			},
			[]aggSetting{
				{120, 600, 2, 0, true},
				{600, 600, 2, 0, true},
			},
			[]Req{
				reqOut("a", 0, 2400, 100, consolidation.Avg, 10, 0, 10, 60, 6),
				reqOut("b", 0, 2400, 100, consolidation.Avg, 30, 0, 30, 60, 2),
				reqOut("c", 0, 2400, 100, consolidation.Avg, 60, 0, 60, 60, 1),
			},
			nil,
		},

		// Similar to above with 3 different raw intervals, but these raw intervals
		// require a little more calculation to get the minimum interval they all fit into.
		// because the minimum interval that they all fit into (300) is greater then the
		// 120second rollup data, the rollups is a better choice.
		{
			[]Req{
				reqRaw("a", 0, 2400, 100, consolidation.Avg, 10),
				reqRaw("b", 0, 2400, 100, consolidation.Avg, 50),
				reqRaw("c", 0, 2400, 100, consolidation.Avg, 60),
			},
			[]aggSetting{
				{120, 600, 2, 0, true},
				{600, 600, 2, 0, true},
			},
			[]Req{
				reqOut("a", 0, 2400, 100, consolidation.Avg, 10, 1, 120, 120, 1),
				reqOut("b", 0, 2400, 100, consolidation.Avg, 50, 1, 120, 120, 1),
				reqOut("c", 0, 2400, 100, consolidation.Avg, 60, 1, 120, 120, 1),
			},
			nil,
		},
		// again with 3 different raw intervals that have a large common interval.
		// With this test, our common raw interval matches our first rollup. Runtime consolidation is expensive
		// so we preference the rollup data.
		{
			[]Req{
				reqRaw("a", 0, 2400, 100, consolidation.Avg, 10),
				reqRaw("b", 0, 2400, 100, consolidation.Avg, 50),
				reqRaw("c", 0, 2400, 100, consolidation.Avg, 60),
			},
			[]aggSetting{
				{300, 600, 2, 0, true},
				{600, 600, 2, 0, true},
			},
			[]Req{
				reqOut("a", 0, 2400, 100, consolidation.Avg, 10, 1, 300, 300, 1),
				reqOut("b", 0, 2400, 100, consolidation.Avg, 50, 1, 300, 300, 1),
				reqOut("c", 0, 2400, 100, consolidation.Avg, 60, 1, 300, 300, 1),
			},
			nil,
		},
		// same but now the first rollup band is disabled
		// raw 2400/300 -> 8 points
		// agg 2 2400/600 -> 4 points
		// best use raw despite the needed consolidation

		{
			[]Req{
				reqRaw("a", 0, 2400, 100, consolidation.Avg, 10),
				reqRaw("b", 0, 2400, 100, consolidation.Avg, 50),
				reqRaw("c", 0, 2400, 100, consolidation.Avg, 60),
			},
			[]aggSetting{
				{300, 600, 2, 0, false},
				{600, 600, 2, 0, true},
			},
			[]Req{
				reqOut("a", 0, 2400, 100, consolidation.Avg, 10, 0, 10, 300, 30),
				reqOut("b", 0, 2400, 100, consolidation.Avg, 50, 0, 50, 300, 6),
				reqOut("c", 0, 2400, 100, consolidation.Avg, 60, 0, 60, 300, 5),
			},
			nil,
		},
		// again with 3 different raw intervals that have a large common interval.
		// With this test, our common raw interval is less then our first rollup so is selected.
		{
			[]Req{
				reqRaw("a", 0, 2400, 100, consolidation.Avg, 10),
				reqRaw("b", 0, 2400, 100, consolidation.Avg, 50),
				reqRaw("c", 0, 2400, 100, consolidation.Avg, 60),
			},
			[]aggSetting{
				{600, 600, 2, 0, true},
				{1200, 1200, 2, 0, true},
			},
			[]Req{
				reqOut("a", 0, 2400, 100, consolidation.Avg, 10, 0, 10, 300, 30),
				reqOut("b", 0, 2400, 100, consolidation.Avg, 50, 0, 50, 300, 6),
				reqOut("c", 0, 2400, 100, consolidation.Avg, 60, 0, 60, 300, 5),
			},
			nil,
		},
		// let's do a realistic one: request 3h worth of data
		// raw means an alignment at 60s interval so:
		// raw -> 10800/60 -> 180 points
		// clearly this fits well within the max points, and it's the highest res,
		// so it's returned.
		{
			[]Req{
				reqRaw("a", 0, 3600*3, 1000, consolidation.Avg, 10),
				reqRaw("b", 0, 3600*3, 1000, consolidation.Avg, 30),
				reqRaw("c", 0, 3600*3, 1000, consolidation.Avg, 60),
			},
			[]aggSetting{
				{600, 21600, 1, 0, true}, // aggregations stored in 6h chunks
				{7200, 21600, 1, 0, true},
				{21600, 21600, 1, 0, true},
			},
			[]Req{
				reqOut("a", 0, 3600*3, 1000, consolidation.Avg, 10, 0, 10, 60, 6),
				reqOut("b", 0, 3600*3, 1000, consolidation.Avg, 30, 0, 30, 60, 2),
				reqOut("c", 0, 3600*3, 1000, consolidation.Avg, 60, 0, 60, 60, 1),
			},
			nil,
		},
		// same but request 6h worth of data
		// raw 21600/60 -> 360. chosen for same reason
		{
			[]Req{
				reqRaw("a", 0, 3600*6, 1000, consolidation.Avg, 10),
				reqRaw("b", 0, 3600*6, 1000, consolidation.Avg, 30),
				reqRaw("c", 0, 3600*6, 1000, consolidation.Avg, 60),
			},
			[]aggSetting{
				{600, 21600, 1, 0, true}, // aggregations stored in 6h chunks
				{7200, 21600, 1, 0, true},
				{21600, 21600, 1, 0, true},
			},
			[]Req{
				reqOut("a", 0, 3600*6, 1000, consolidation.Avg, 10, 0, 10, 60, 6),
				reqOut("b", 0, 3600*6, 1000, consolidation.Avg, 30, 0, 30, 60, 2),
				reqOut("c", 0, 3600*6, 1000, consolidation.Avg, 60, 0, 60, 60, 1),
			},
			nil,
		},
		// same but request 9h worth of data
		// raw 32400/60 -> 540. chosen for same reason
		{
			[]Req{
				reqRaw("a", 0, 3600*9, 1000, consolidation.Avg, 10),
				reqRaw("b", 0, 3600*9, 1000, consolidation.Avg, 30),
				reqRaw("c", 0, 3600*9, 1000, consolidation.Avg, 60),
			},
			[]aggSetting{
				{600, 21600, 1, 0, true}, // aggregations stored in 6h chunks
				{7200, 21600, 1, 0, true},
				{21600, 21600, 1, 0, true},
			},
			[]Req{
				reqOut("a", 0, 3600*9, 1000, consolidation.Avg, 10, 0, 10, 60, 6),
				reqOut("b", 0, 3600*9, 1000, consolidation.Avg, 30, 0, 30, 60, 2),
				reqOut("c", 0, 3600*9, 1000, consolidation.Avg, 60, 0, 60, 60, 1),
			},
			nil,
		},
		// same but request 24h worth of data
		// raw 86400/60 -> 1440
		// agg1 86400/600 -> 144 points -> best choice
		{
			[]Req{
				reqRaw("a", 0, 3600*24, 1000, consolidation.Avg, 10),
				reqRaw("b", 0, 3600*24, 1000, consolidation.Avg, 30),
				reqRaw("c", 0, 3600*24, 1000, consolidation.Avg, 60),
			},
			[]aggSetting{
				{600, 21600, 1, 0, true}, // aggregations stored in 6h chunks
				{7200, 21600, 1, 0, true},
				{21600, 21600, 1, 0, true},
			},
			[]Req{
				reqOut("a", 0, 3600*24, 1000, consolidation.Avg, 10, 1, 600, 600, 1),
				reqOut("b", 0, 3600*24, 1000, consolidation.Avg, 30, 1, 600, 600, 1),
				reqOut("c", 0, 3600*24, 1000, consolidation.Avg, 60, 1, 600, 600, 1),
			},
			nil,
		},
		// same but now let's request 2 weeks worth of data.
		// not using raw is a no brainer.
		// agg1 3600*24*7 / 600 = 1008 points, which is too many, so must also do runtime consolidation and bring it back to 504
		// agg2 3600*24*7 / 7200 = 84 points -> too far below maxdatapoints, better to do agg1 with runtime consol
		{
			[]Req{
				reqRaw("a", 0, 3600*24*7, 1000, consolidation.Avg, 10),
				reqRaw("b", 0, 3600*24*7, 1000, consolidation.Avg, 30),
				reqRaw("c", 0, 3600*24*7, 1000, consolidation.Avg, 60),
			},
			[]aggSetting{
				{600, 21600, 1, 0, true}, // aggregations stored in 6h chunks
				{7200, 21600, 1, 0, true},
				{21600, 21600, 1, 0, true},
			},
			[]Req{
				reqOut("a", 0, 3600*24*7, 1000, consolidation.Avg, 10, 1, 600, 1200, 2),
				reqOut("b", 0, 3600*24*7, 1000, consolidation.Avg, 30, 1, 600, 1200, 2),
				reqOut("c", 0, 3600*24*7, 1000, consolidation.Avg, 60, 1, 600, 1200, 2),
			},
			nil,
		},
		// let's request 1 year of data
		// raw 3600*24*365/60 -> 525600
		// agg1 3600*24*365/600 -> 52560
		// agg2 3600*24*365/7200 -> 4380
		// agg3 3600*24*365/21600 -> 1460
		// clearly agg3 is the best, and we have to runtime consolidate wih aggNum 2
		{
			[]Req{
				reqRaw("a", 0, 3600*24*365, 1000, consolidation.Avg, 10),
				reqRaw("b", 0, 3600*24*365, 1000, consolidation.Avg, 30),
				reqRaw("c", 0, 3600*24*365, 1000, consolidation.Avg, 60),
			},
			[]aggSetting{
				{600, 21600, 1, 0, true}, // aggregations stored in 6h chunks
				{7200, 21600, 1, 0, true},
				{21600, 21600, 1, 0, true},
			},
			[]Req{
				reqOut("a", 0, 3600*24*365, 1000, consolidation.Avg, 10, 3, 21600, 43200, 2),
				reqOut("b", 0, 3600*24*365, 1000, consolidation.Avg, 30, 3, 21600, 43200, 2),
				reqOut("c", 0, 3600*24*365, 1000, consolidation.Avg, 60, 3, 21600, 43200, 2),
			},
			nil,
		},
		// ditto but disable agg3
		// so we have to use agg2 with aggNum of 5
		{
			[]Req{
				reqRaw("a", 0, 3600*24*365, 1000, consolidation.Avg, 10),
				reqRaw("b", 0, 3600*24*365, 1000, consolidation.Avg, 30),
				reqRaw("c", 0, 3600*24*365, 1000, consolidation.Avg, 60),
			},
			[]aggSetting{
				{600, 21600, 1, 0, true}, // aggregations stored in 6h chunks
				{7200, 21600, 1, 0, true},
				{21600, 21600, 1, 0, false},
			},
			[]Req{
				reqOut("a", 0, 3600*24*365, 1000, consolidation.Avg, 10, 2, 7200, 36000, 5),
				reqOut("b", 0, 3600*24*365, 1000, consolidation.Avg, 30, 2, 7200, 36000, 5),
				reqOut("c", 0, 3600*24*365, 1000, consolidation.Avg, 60, 2, 7200, 36000, 5),
			},
			nil,
		},
		// now let's request 1 year of data again, but without actually having any aggregation bands (wowa don't do this)
		// raw 3600*24*365/60 -> 525600
		// we need an aggNum of 526 to keep this under 1000 points
		{
			[]Req{
				reqRaw("a", 0, 3600*24*365, 1000, consolidation.Avg, 10),
				reqRaw("b", 0, 3600*24*365, 1000, consolidation.Avg, 30),
				reqRaw("c", 0, 3600*24*365, 1000, consolidation.Avg, 60),
			},
			[]aggSetting{},
			[]Req{
				reqOut("a", 0, 3600*24*365, 1000, consolidation.Avg, 10, 0, 10, 31560, 526*6),
				reqOut("b", 0, 3600*24*365, 1000, consolidation.Avg, 30, 0, 30, 31560, 526*2),
				reqOut("c", 0, 3600*24*365, 1000, consolidation.Avg, 60, 0, 60, 31560, 526),
			},
			nil,
		},
		// same thing but if the metrics have the same resolution
		// raw 3600*24*365/60 -> 525600
		// we need an aggNum of 526 to keep this under 1000 points
		{
			[]Req{
				reqRaw("a", 0, 3600*24*365, 1000, consolidation.Avg, 30),
				reqRaw("b", 0, 3600*24*365, 1000, consolidation.Avg, 30),
				reqRaw("c", 0, 3600*24*365, 1000, consolidation.Avg, 30),
			},
			[]aggSetting{},
			[]Req{
				reqOut("a", 0, 3600*24*365, 1000, consolidation.Avg, 30, 0, 30, 31560, 526*2),
				reqOut("b", 0, 3600*24*365, 1000, consolidation.Avg, 30, 0, 30, 31560, 526*2),
				reqOut("c", 0, 3600*24*365, 1000, consolidation.Avg, 30, 0, 30, 31560, 526*2),
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

func randFloats() []schema.Point {
	// let's just do the "odd" case, since the non-odd will be sufficiently close
	ret := make([]schema.Point, 1000001)
	for i := 0; i < len(ret); i++ {
		ret[i] = schema.Point{rand.Float64(), uint32(i)}
	}
	return ret
}

func randFloatsWithNulls() []schema.Point {
	// let's just do the "odd" case, since the non-odd will be sufficiently close
	ret := make([]schema.Point, 1000001)
	for i := 0; i < len(ret); i++ {
		if i%2 == 0 {
			ret[i] = schema.Point{math.NaN(), uint32(i)}
		} else {
			ret[i] = schema.Point{rand.Float64(), uint32(i)}
		}
	}
	return ret
}

// each "operation" is a consolidation of 1M+1 points
func BenchmarkConsolidateAvgRand1M_1(b *testing.B) {
	benchmarkConsolidate(randFloats, 1, consolidation.Avg, b)
}
func BenchmarkConsolidateAvgRandWithNulls1M_1(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 1, consolidation.Avg, b)
}
func BenchmarkConsolidateAvgRand1M_2(b *testing.B) {
	benchmarkConsolidate(randFloats, 2, consolidation.Avg, b)
}
func BenchmarkConsolidateAvgRandWithNulls1M_2(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 2, consolidation.Avg, b)
}
func BenchmarkConsolidateAvgRand1M_25(b *testing.B) {
	benchmarkConsolidate(randFloats, 25, consolidation.Avg, b)
}
func BenchmarkConsolidateAvgRandWithNulls1M_25(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 25, consolidation.Avg, b)
}
func BenchmarkConsolidateAvgRand1M_100(b *testing.B) {
	benchmarkConsolidate(randFloats, 100, consolidation.Avg, b)
}
func BenchmarkConsolidateAvgRandWithNulls1M_100(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 100, consolidation.Avg, b)
}

func BenchmarkConsolidateMinRand1M_1(b *testing.B) {
	benchmarkConsolidate(randFloats, 1, consolidation.Min, b)
}
func BenchmarkConsolidateMinRandWithNulls1M_1(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 1, consolidation.Min, b)
}
func BenchmarkConsolidateMinRand1M_2(b *testing.B) {
	benchmarkConsolidate(randFloats, 2, consolidation.Min, b)
}
func BenchmarkConsolidateMinRandWithNulls1M_2(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 2, consolidation.Min, b)
}
func BenchmarkConsolidateMinRand1M_25(b *testing.B) {
	benchmarkConsolidate(randFloats, 25, consolidation.Min, b)
}
func BenchmarkConsolidateMinRandWithNulls1M_25(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 25, consolidation.Min, b)
}
func BenchmarkConsolidateMinRand1M_100(b *testing.B) {
	benchmarkConsolidate(randFloats, 100, consolidation.Min, b)
}
func BenchmarkConsolidateMinRandWithNulls1M_100(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 100, consolidation.Min, b)
}

func BenchmarkConsolidateMaxRand1M_1(b *testing.B) {
	benchmarkConsolidate(randFloats, 1, consolidation.Max, b)
}
func BenchmarkConsolidateMaxRandWithNulls1M_1(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 1, consolidation.Max, b)
}
func BenchmarkConsolidateMaxRand1M_2(b *testing.B) {
	benchmarkConsolidate(randFloats, 2, consolidation.Max, b)
}
func BenchmarkConsolidateMaxRandWithNulls1M_2(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 2, consolidation.Max, b)
}
func BenchmarkConsolidateMaxRand1M_25(b *testing.B) {
	benchmarkConsolidate(randFloats, 25, consolidation.Max, b)
}
func BenchmarkConsolidateMaxRandWithNulls1M_25(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 25, consolidation.Max, b)
}
func BenchmarkConsolidateMaxRand1M_100(b *testing.B) {
	benchmarkConsolidate(randFloats, 100, consolidation.Max, b)
}
func BenchmarkConsolidateMaxRandWithNulls1M_100(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 100, consolidation.Max, b)
}

func BenchmarkConsolidateSumRand1M_1(b *testing.B) {
	benchmarkConsolidate(randFloats, 1, consolidation.Sum, b)
}
func BenchmarkConsolidateSumRandWithNulls1M_1(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 1, consolidation.Sum, b)
}
func BenchmarkConsolidateSumRand1M_2(b *testing.B) {
	benchmarkConsolidate(randFloats, 2, consolidation.Sum, b)
}
func BenchmarkConsolidateSumRandWithNulls1M_2(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 2, consolidation.Sum, b)
}
func BenchmarkConsolidateSumRand1M_25(b *testing.B) {
	benchmarkConsolidate(randFloats, 25, consolidation.Sum, b)
}
func BenchmarkConsolidateSumRandWithNulls1M_25(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 25, consolidation.Sum, b)
}
func BenchmarkConsolidateSumRand1M_100(b *testing.B) {
	benchmarkConsolidate(randFloats, 100, consolidation.Sum, b)
}
func BenchmarkConsolidateSumRandWithNulls1M_100(b *testing.B) {
	benchmarkConsolidate(randFloatsWithNulls, 100, consolidation.Sum, b)
}

var dummy []schema.Point

func benchmarkConsolidate(fn func() []schema.Point, aggNum uint32, consolidator consolidation.Consolidator, b *testing.B) {
	var l int
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		in := fn()
		l = len(in)
		b.StartTimer()
		ret := consolidate(in, aggNum, consolidator)
		dummy = ret
	}
	b.SetBytes(int64(l * 12))
}

func BenchmarkFix1M(b *testing.B) {
	var l int
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		in := randFloats()
		l = len(in)
		b.StartTimer()
		out := fix(in, 0, 1000001, 1)
		dummy = out
	}
	b.SetBytes(int64(l * 12))
}

func BenchmarkDivide1M(b *testing.B) {
	var l int
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ina := randFloats()
		inb := randFloats()
		l = len(ina)
		b.StartTimer()
		out := divide(ina, inb)
		dummy = out
	}
	b.SetBytes(int64(l * 12))
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
