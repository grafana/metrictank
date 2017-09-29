package consolidation

import (
	"testing"

	"github.com/grafana/metrictank/test"
	"gopkg.in/raintank/schema.v1"
)

type testCase struct {
	in     []schema.Point
	consol Consolidator
	num    uint32
	out    []schema.Point
}

func validate(cases []testCase, t *testing.T) {
	for i, c := range cases {
		out := Consolidate(c.in, c.num, c.consol)
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
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
			Avg,
			1,
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
		},
		{
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
			Avg,
			3,
			[]schema.Point{
				{Val: 2, Ts: 1449178151},
				{Val: 4, Ts: 1449178181}, // see comment below
			},
		},
		{
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
			},
			Avg,
			1,
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
			},
		},
		{
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
			},
			Avg,
			2,
			[]schema.Point{
				{Val: 1.5, Ts: 1449178141},
				{Val: 3, Ts: 1449178161}, // note: we choose the next ts here for even spacing (important for further processing/parsing/handing off), even though that point is missing
			},
		},
		{
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
			},
			Avg,
			3,
			[]schema.Point{
				{Val: 2, Ts: 1449178151},
			},
		},
	}
	validate(cases, t)
}
func TestConsolidationFunctions(t *testing.T) {
	cases := []testCase{
		{
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
			Avg,
			2,
			[]schema.Point{
				{Val: 1.5, Ts: 1449178141},
				{Val: 3.5, Ts: 1449178161},
			},
		},
		{
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
			Cnt,
			2,
			[]schema.Point{
				{Val: 2, Ts: 1449178141},
				{Val: 2, Ts: 1449178161},
			},
		},
		{
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
			Lst,
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
			Min,
			2,
			[]schema.Point{
				{Val: 1, Ts: 1449178141},
				{Val: 3, Ts: 1449178161},
			},
		},
		{
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
			Max,
			2,
			[]schema.Point{
				{Val: 2, Ts: 1449178141},
				{Val: 4, Ts: 1449178161},
			},
		},
		{
			[]schema.Point{
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
			Sum,
			2,
			[]schema.Point{
				{Val: 3, Ts: 1449178141},
				{Val: 7, Ts: 1449178161},
			},
		},
	}
	validate(cases, t)
}

func TestConsolidateStableNoAgg(t *testing.T) {
	testConsolidateStable(
		[]schema.Point{
			{Val: 1, Ts: 10},
			{Val: 2, Ts: 20},
			{Val: 3, Ts: 30},
			{Val: 4, Ts: 40},
		},
		10,
		1,
		[]schema.Point{
			{Val: 10, Ts: 40},
		},
		40,
		t)
}

func TestConsolidateStableNoTrimDueToNotManyPoints(t *testing.T) {
	testConsolidateStable(
		[]schema.Point{
			{Val: 1, Ts: 20},
			{Val: 2, Ts: 30},
			{Val: 3, Ts: 40},
			{Val: 4, Ts: 50},
		},
		10,
		1,
		[]schema.Point{
			{Val: 10, Ts: 50},
		},
		40,
		t)
}
func TestConsolidateStableShouldTrim(t *testing.T) {
	testConsolidateStable(
		// more points, we should trim to get proper alignment
		[]schema.Point{
			{Val: 1, Ts: 20},
			{Val: 2, Ts: 30},
			{Val: 3, Ts: 40},
			{Val: 4, Ts: 50},
			{Val: 5, Ts: 60},
			{Val: 6, Ts: 70},
		},
		10,
		3,
		[]schema.Point{
			{Val: 5, Ts: 40},
			{Val: 9, Ts: 60},
			{Val: 6, Ts: 80},
		},
		20,
		t)
}
func TestConsolidateStableShouldBeStableWithPrevious(t *testing.T) {
	testConsolidateStable(
		// and now we learn why
		// one point out of the window and a new one at the back
		// should result in the same consolidated points for everything in the middle
		// as compared to the previous test
		[]schema.Point{
			{Val: 2, Ts: 30},
			{Val: 3, Ts: 40},
			{Val: 4, Ts: 50},
			{Val: 5, Ts: 60},
			{Val: 6, Ts: 70},
			{Val: 7, Ts: 80},
		},
		10,
		3,
		[]schema.Point{
			{Val: 5, Ts: 40},
			{Val: 9, Ts: 60},
			{Val: 13, Ts: 80},
		},
		20,
		t)
}

func TestConsolidateStableABitMoreData(t *testing.T) {
	testConsolidateStable(
		// another trimming example with a bit more data
		// logic is as follows: 13 points, mdp 3 => aggregate every 5
		// so first agg point should be 10,20,30,40,50, which is incomplete
		// so nudge it away.
		// this actually leaves us with only 9 points, so in theory we could
		// use more and smaller buckets of 3 or 4 points,
		// but this would lead to the problem again
		// of data jumping around across refreshes (as subsequent requests may not
		// nudge as much data, and require larger buckets), so this is better.
		[]schema.Point{
			{Val: 2, Ts: 20},   // incomplete. shall be nudged out
			{Val: 3, Ts: 30},   // incomplete. shall be nudged out
			{Val: 4, Ts: 40},   // incomplete. shall be nudged out
			{Val: 5, Ts: 50},   // incomplete. shall be nudged out
			{Val: 6, Ts: 60},   // bucket 1
			{Val: 7, Ts: 70},   // bucket 1
			{Val: 8, Ts: 80},   // bucket 1
			{Val: 9, Ts: 90},   // bucket 1
			{Val: 10, Ts: 100}, // bucket 1
			{Val: 11, Ts: 110}, // bucket 2
			{Val: 12, Ts: 120}, // bucket 2
			{Val: 13, Ts: 130}, // bucket 2
			{Val: 14, Ts: 140}, // bucket 2
		},
		10,
		3,
		[]schema.Point{
			{Val: 40, Ts: 100},
			{Val: 50, Ts: 150},
		},
		50,
		t)
}
func TestConsolidateStableABitMoreDataEven(t *testing.T) {
	testConsolidateStable(
		// now we actually have a clean start at 10, so we can incorporate it
		[]schema.Point{
			{Val: 1, Ts: 10},   // bucket 1
			{Val: 2, Ts: 20},   // bucket 1
			{Val: 3, Ts: 30},   // bucket 1
			{Val: 4, Ts: 40},   // bucket 1
			{Val: 5, Ts: 50},   // bucket 1
			{Val: 6, Ts: 60},   // bucket 2
			{Val: 7, Ts: 70},   // bucket 2
			{Val: 8, Ts: 80},   // bucket 2
			{Val: 9, Ts: 90},   // bucket 2
			{Val: 10, Ts: 100}, // bucket 2
			{Val: 11, Ts: 110}, // bucket 3
			{Val: 12, Ts: 120}, // bucket 3
			{Val: 13, Ts: 130}, // bucket 3
			{Val: 14, Ts: 140}, // bucket 3
		},
		10,
		3,
		[]schema.Point{
			{Val: 15, Ts: 50},
			{Val: 40, Ts: 100},
			{Val: 50, Ts: 150},
		},
		50,
		t)
}
func testConsolidateStable(in []schema.Point, inInt uint32, mdp uint32, expOut []schema.Point, expOutInt uint32, t *testing.T) {
	out, outInt := ConsolidateStable(in, inInt, mdp, Sum)
	if outInt != expOutInt {
		t.Fatalf("output interval mismatch: expected: %v, got: %v", expOutInt, outInt)
	}
	if len(out) != len(expOut) {
		t.Fatalf("output mismatch: expected: %v, got: %v", expOut, out)
	}
	for j := 0; j < len(out); j++ {
		if out[j] != expOut[j] {
			t.Fatalf("output mismatch: expected: %v, got: %v", expOut, out)
		}
	}
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
		every := AggEvery(c.numPoints, c.maxDataPoints)
		if every != c.every {
			t.Fatalf("output for testcase %d mismatch: expected: %v, got: %v", i, c.every, every)
		}
	}
}

// each "operation" is a consolidation of 1M+1 points
func BenchmarkConsolidateAvgRand1M_1(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 1, Avg, b)
}
func BenchmarkConsolidateAvgRandWithNulls1M_1(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 1, Avg, b)
}
func BenchmarkConsolidateAvgRand1M_2(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 2, Avg, b)
}
func BenchmarkConsolidateAvgRandWithNulls1M_2(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 2, Avg, b)
}
func BenchmarkConsolidateAvgRand1M_25(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 25, Avg, b)
}
func BenchmarkConsolidateAvgRandWithNulls1M_25(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 25, Avg, b)
}
func BenchmarkConsolidateAvgRand1M_100(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 100, Avg, b)
}
func BenchmarkConsolidateAvgRandWithNulls1M_100(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 100, Avg, b)
}

func BenchmarkConsolidateMinRand1M_1(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 1, Min, b)
}
func BenchmarkConsolidateMinRandWithNulls1M_1(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 1, Min, b)
}
func BenchmarkConsolidateMinRand1M_2(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 2, Min, b)
}
func BenchmarkConsolidateMinRandWithNulls1M_2(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 2, Min, b)
}
func BenchmarkConsolidateMinRand1M_25(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 25, Min, b)
}
func BenchmarkConsolidateMinRandWithNulls1M_25(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 25, Min, b)
}
func BenchmarkConsolidateMinRand1M_100(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 100, Min, b)
}
func BenchmarkConsolidateMinRandWithNulls1M_100(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 100, Min, b)
}

func BenchmarkConsolidateMaxRand1M_1(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 1, Max, b)
}
func BenchmarkConsolidateMaxRandWithNulls1M_1(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 1, Max, b)
}
func BenchmarkConsolidateMaxRand1M_2(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 2, Max, b)
}
func BenchmarkConsolidateMaxRandWithNulls1M_2(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 2, Max, b)
}
func BenchmarkConsolidateMaxRand1M_25(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 25, Max, b)
}
func BenchmarkConsolidateMaxRandWithNulls1M_25(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 25, Max, b)
}
func BenchmarkConsolidateMaxRand1M_100(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 100, Max, b)
}
func BenchmarkConsolidateMaxRandWithNulls1M_100(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 100, Max, b)
}

func BenchmarkConsolidateSumRand1M_1(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 1, Sum, b)
}
func BenchmarkConsolidateSumRandWithNulls1M_1(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 1, Sum, b)
}
func BenchmarkConsolidateSumRand1M_2(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 2, Sum, b)
}
func BenchmarkConsolidateSumRandWithNulls1M_2(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 2, Sum, b)
}
func BenchmarkConsolidateSumRand1M_25(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 25, Sum, b)
}
func BenchmarkConsolidateSumRandWithNulls1M_25(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 25, Sum, b)
}
func BenchmarkConsolidateSumRand1M_100(b *testing.B) {
	benchmarkConsolidate(test.RandFloats1M, 100, Sum, b)
}
func BenchmarkConsolidateSumRandWithNulls1M_100(b *testing.B) {
	benchmarkConsolidate(test.RandFloatsWithNulls1M, 100, Sum, b)
}

var dummy []schema.Point

func benchmarkConsolidate(fn func() []schema.Point, aggNum uint32, consolidator Consolidator, b *testing.B) {
	var l int
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		in := fn()
		l = len(in)
		b.StartTimer()
		ret := Consolidate(in, aggNum, consolidator)
		dummy = ret
	}
	b.SetBytes(int64(l * 12))
}
