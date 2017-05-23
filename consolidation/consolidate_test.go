package consolidation

import (
	"testing"

	"github.com/raintank/metrictank/test"
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
