package api

import (
	"fmt"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/cache"
	"github.com/raintank/metrictank/mdata/cache/accnt"
	"github.com/raintank/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

type testCase struct {
	in     []schema.Point
	consol consolidation.Consolidator
	num    uint32
	out    []schema.Point
}

func init() {
	cluster.Init("default", "test", time.Now(), "http", 6060)
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
				{Val: 1, Ts: 1449178131},
				{Val: 2, Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
			consolidation.Avg,
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
			consolidation.Avg,
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
			consolidation.Avg,
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
			consolidation.Avg,
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
			consolidation.Avg,
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
			consolidation.Avg,
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
			consolidation.Cnt,
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
			consolidation.Min,
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
			consolidation.Max,
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
			consolidation.Sum,
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
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
			[]schema.Point{
				{Val: 2, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 1, Ts: 30},
			},
			[]schema.Point{
				{Val: 0.5, Ts: 10},
				{Val: 1, Ts: 20},
				{Val: 3, Ts: 30},
			},
		},
		{
			[]schema.Point{
				{Val: 100, Ts: 10},
				{Val: 5000, Ts: 20},
				{Val: 150.5, Ts: 30},
				{Val: 150.5, Ts: 40},
			},
			[]schema.Point{
				{Val: 2, Ts: 10},
				{Val: 0.5, Ts: 20},
				{Val: 2, Ts: 30},
				{Val: 0.5, Ts: 40},
			},
			[]schema.Point{
				{Val: 50, Ts: 10},
				{Val: 10000, Ts: 20},
				{Val: 75.25, Ts: 30},
				{Val: 301, Ts: 40},
			},
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
	interval uint32
	out      []schema.Point
}

func nullPoints(from, to, interval uint32) []schema.Point {
	out := make([]schema.Point, 0)
	for i := from; i < to; i += interval {
		out = append(out, schema.Point{Val: math.NaN(), Ts: i})
	}
	return out
}

func TestFix(t *testing.T) {
	cases := []fixc{
		{
			// the most standard simple case
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
			10,
			31,
			10,
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
		},
		{
			// almost... need Nan in front
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
			1,
			31,
			10,
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
		},
		{
			// need Nan in front
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
			0,
			31,
			10,
			[]schema.Point{
				{Val: math.NaN(), Ts: 0},
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
		},
		{
			// almost..need Nan in back
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
			10,
			40,
			10,
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
		},
		{
			// need Nan in back
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
			},
			10,
			41,
			10,
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 3, Ts: 30},
				{Val: math.NaN(), Ts: 40},
			},
		},
		{
			// need Nan in middle
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 3, Ts: 30},
			},
			10,
			31,
			10,
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: 3, Ts: 30},
			},
		},
		{
			// need Nan everywhere
			[]schema.Point{
				{Val: 2, Ts: 20},
				{Val: 4, Ts: 40},
				{Val: 7, Ts: 70},
			},
			0,
			90,
			10,
			[]schema.Point{
				{Val: math.NaN(), Ts: 0},
				{Val: math.NaN(), Ts: 10},
				{Val: 2, Ts: 20},
				{Val: math.NaN(), Ts: 30},
				{Val: 4, Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: math.NaN(), Ts: 60},
				{Val: 7, Ts: 70},
				{Val: math.NaN(), Ts: 80},
			},
		},
		{
			// too much data. note that there are multiple satisfactory solutions here. this is just one of them.
			[]schema.Point{
				{Val: 10, Ts: 10},
				{Val: 14, Ts: 14},
				{Val: 20, Ts: 20},
				{Val: 26, Ts: 26},
				{Val: 35, Ts: 35},
			},
			10,
			41,
			10,
			[]schema.Point{
				{Val: 10, Ts: 10},
				{Val: 14, Ts: 20},
				{Val: 26, Ts: 30},
				{Val: 35, Ts: 40},
			},
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
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: 2, Ts: 19},
			},
			10,
			31,
			10,
			[]schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
				{Val: math.NaN(), Ts: 30},
			},
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
			[]schema.Point{
				{Val: math.NaN(), Ts: 1458966240},
			},
		},
	}

	for i, c := range cases {
		got := Fix(c.in, c.from, c.to, c.interval)

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

type pbCase struct {
	ts       uint32
	span     uint32
	boundary uint32
}

func TestPrevBoundary(t *testing.T) {
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
		if ret := prevBoundary(c.ts, c.span); ret != c.boundary {
			t.Fatalf("prevBoundary for ts %d with span %d should be %d, not %d", c.ts, c.span, c.boundary, ret)
		}
	}
}

// TestGetSeriesFixed assures that series data is returned in proper form.
func TestGetSeriesFixed(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	store := mdata.NewDevnullStore()
	metrics := mdata.NewAggMetrics(store, &cache.MockCache{}, 600, 10, 0, 0, 0, 0, []mdata.AggSetting{})
	srv, _ := NewServer()
	srv.BindBackendStore(store)
	srv.BindMemoryStore(metrics)

	// the tests below cycles through every possible combination of:
	// * every possible data   offset (against its quantized version)       e.g. offset between 0 and interval-1
	// * every possible `from` offset (against its quantized query results) e.g. offset between 0 and interval-1
	// * every possible `to`   offset (against its quantized query results) e.g. offset between 0 and interval-1
	// and asserts that we get the appropriate data back in all possible scenarios.

	expected := []schema.Point{
		{Val: 20, Ts: 20},
		{Val: 30, Ts: 30},
	}

	for offset := uint32(1); offset <= 10; offset++ {
		for from := uint32(11); from <= 20; from++ { // should always yield result with first point at 20 (because from is inclusive)
			for to := uint32(31); to <= 40; to++ { // should always yield result with last point at 30 (because to is exclusive)
				name := fmt.Sprintf("case.data.offset.%d.query:%d-%d", offset, from, to)

				metric := metrics.GetOrCreate(name)
				metric.Add(offset, 10)    // this point will always be quantized to 10
				metric.Add(10+offset, 20) // this point will always be quantized to 20, so it should be selected
				metric.Add(20+offset, 30) // this point will always be quantized to 30, so it should be selected
				metric.Add(30+offset, 40) // this point will always be quantized to 40
				metric.Add(40+offset, 50) // this point will always be quantized to 50
				req := models.NewReq(name, name, from, to, 1000, 10, consolidation.Avg, cluster.Manager.ThisNode())
				req.ArchInterval = 10
				points := srv.getSeriesFixed(req, consolidation.None)
				if !reflect.DeepEqual(expected, points) {
					t.Errorf("case %q - exp: %v - got %v", name, expected, points)
				}
			}
		}
	}
}

type alignCase struct {
	reqs        []models.Req
	aggSettings mdata.AggSettings
	outReqs     []models.Req
	outErr      error
}

func reqRaw(key string, from, to, maxPoints, rawInterval uint32, consolidator consolidation.Consolidator) models.Req {
	req := models.NewReq(key, key, from, to, maxPoints, rawInterval, consolidator, cluster.Manager.ThisNode())
	return req
}
func reqOut(key string, from, to, maxPoints, rawInterval uint32, consolidator consolidation.Consolidator, archive int, archInterval, outInterval, aggNum uint32) models.Req {
	req := models.NewReq(key, key, from, to, maxPoints, rawInterval, consolidator, cluster.Manager.ThisNode())
	req.Archive = archive
	req.ArchInterval = archInterval
	req.OutInterval = outInterval
	req.AggNum = aggNum
	return req
}

func TestAlignRequests(t *testing.T) {
	input := []alignCase{
		{
			// real example seen with alerting queries
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},
		{
			// raw would be 3600/10=360 points, agg1 3600/60=60. raw is best cause it provides most points
			// and still under the max points limit.
			[]models.Req{
				reqRaw("a", 0, 3600, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 3600, 800, 10, consolidation.Avg),
				reqRaw("c", 0, 3600, 800, 10, consolidation.Avg),
			},
			// span, chunkspan, numchunks, ttl, ready
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(60, 600, 2, 0, true),
					mdata.NewAggSetting(120, 600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
				reqOut("b", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
				reqOut("c", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
			},
			nil,
		},
		{
			// same as before, but agg1 disabled. just to make sure it still behaves the same.
			[]models.Req{
				reqRaw("a", 0, 3600, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 3600, 800, 10, consolidation.Avg),
				reqRaw("c", 0, 3600, 800, 10, consolidation.Avg),
			},
			// span, chunkspan, numchunks, ttl, ready
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(60, 600, 2, 0, false),
					mdata.NewAggSetting(120, 600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
				reqOut("b", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
				reqOut("c", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
			},
			nil,
		},
		{
			// now we request 0-2400, with max datapoints 100.
			// raw: 2400/10 -> 240pts -> needs runtime consolidation
			// agg1: 2400/60 -> 40 pts, good candidate,
			// though 40pts is 2.5x smaller than the maxPoints target (100 points)
			// raw's 240 pts is only 2.4x larger then 100pts, so it is selected.
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 10, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(60, 600, 2, 0, true),
					mdata.NewAggSetting(120, 600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 30, 3),
				reqOut("b", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 30, 3),
				reqOut("c", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 30, 3),
			},
			nil,
		},
		// same thing as above, but now we set max points to 39. So now the 240pts
		// raw: 2400/10 -> 240 pts is 6.15x our target of 39pts
		// agg1 2400/120 -> 20 pts is only 1.95x smaller then our target 39pts so it is
		// selected.
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 39, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 39, 10, consolidation.Avg),
				reqRaw("c", 0, 2400, 39, 10, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, true),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 39, 10, consolidation.Avg, 1, 120, 120, 1),
				reqOut("b", 0, 2400, 39, 10, consolidation.Avg, 1, 120, 120, 1),
				reqOut("c", 0, 2400, 39, 10, consolidation.Avg, 1, 120, 120, 1),
			},
			nil,
		},
		// same as above, except now the 120s band is disabled.
		// raw: 2400/10 -> 240 pts is 6.15x our target of 39pts
		// agg1 2400/600 -> 4 pts is about 10x smaller so we prefer raw again
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 39, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 39, 10, consolidation.Avg),
				reqRaw("c", 0, 2400, 39, 10, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, false),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			// rawInterval, archive, archInterval, outInterval, aggNum
			[]models.Req{
				reqOut("a", 0, 2400, 39, 10, consolidation.Avg, 0, 10, 70, 7),
				reqOut("b", 0, 2400, 39, 10, consolidation.Avg, 0, 10, 70, 7),
				reqOut("c", 0, 2400, 39, 10, consolidation.Avg, 0, 10, 70, 7),
			},
			nil,
		},
		// same as above, 120s band is still disabled. but we query for a longer range
		// raw: 24000/10 -> 2400 pts is 61.5x our target of 39pts
		// agg2 24000/600 -> 40 pts is just too large, but we can make it work with runtime
		// consolidation
		{
			[]models.Req{
				reqRaw("a", 0, 24000, 39, 10, consolidation.Avg),
				reqRaw("b", 0, 24000, 39, 10, consolidation.Avg),
				reqRaw("c", 0, 24000, 39, 10, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, false),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			// rawInterval, archive, archInterval, outInterval, aggNum
			[]models.Req{
				reqOut("a", 0, 24000, 39, 10, consolidation.Avg, 2, 600, 1200, 2),
				reqOut("b", 0, 24000, 39, 10, consolidation.Avg, 2, 600, 1200, 2),
				reqOut("c", 0, 24000, 39, 10, consolidation.Avg, 2, 600, 1200, 2),
			},
			nil,
		},
		// now something a bit different. 3 different raw intervals, but same aggregation settings.
		// raw is here best again but all series need to be at a step of 60
		// so runtime consolidation is needed, we'll get 40 points for each metric
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 30, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, true),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 2400, 100, 30, consolidation.Avg, 0, 30, 60, 2),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},

		// Similar to above with 3 different raw intervals, but these raw intervals
		// require a little more calculation to get the minimum interval they all fit into.
		// because the minimum interval that they all fit into (300) is greater then the
		// 120second rollup data, the rollups is a better choice.
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 50, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, true),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 1, 120, 120, 1),
				reqOut("b", 0, 2400, 100, 50, consolidation.Avg, 1, 120, 120, 1),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 1, 120, 120, 1),
			},
			nil,
		},
		// again with 3 different raw intervals that have a large common interval.
		// With this test, our common raw interval matches our first rollup. Runtime consolidation is expensive
		// so we preference the rollup data.
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 50, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(300, 600, 2, 0, true),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 1, 300, 300, 1),
				reqOut("b", 0, 2400, 100, 50, consolidation.Avg, 1, 300, 300, 1),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 1, 300, 300, 1),
			},
			nil,
		},
		// same but now the first rollup band is disabled
		// raw 2400/300 -> 8 points
		// agg 2 2400/600 -> 4 points
		// best use raw despite the needed consolidation

		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 50, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(300, 600, 2, 0, false),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 300, 30),
				reqOut("b", 0, 2400, 100, 50, consolidation.Avg, 0, 50, 300, 6),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 0, 60, 300, 5),
			},
			nil,
		},
		// again with 3 different raw intervals that have a large common interval.
		// With this test, our common raw interval is less then our first rollup so is selected.
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 50, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 600, 2, 0, true),
					mdata.NewAggSetting(1200, 1200, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 300, 30),
				reqOut("b", 0, 2400, 100, 50, consolidation.Avg, 0, 50, 300, 6),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 0, 60, 300, 5),
			},
			nil,
		},
		// let's do a realistic one: request 3h worth of data
		// raw means an alignment at 60s interval so:
		// raw -> 10800/60 -> 180 points
		// clearly this fits well within the max points, and it's the highest res,
		// so it's returned.
		{
			[]models.Req{
				reqRaw("a", 0, 3600*3, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*3, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*3, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*3, 1000, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 3600*3, 1000, 30, consolidation.Avg, 0, 30, 60, 2),
				reqOut("c", 0, 3600*3, 1000, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},
		// same but request 6h worth of data
		// raw 21600/60 -> 360. chosen for same reason
		{
			[]models.Req{
				reqRaw("a", 0, 3600*6, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*6, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*6, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*6, 1000, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 3600*6, 1000, 30, consolidation.Avg, 0, 30, 60, 2),
				reqOut("c", 0, 3600*6, 1000, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},
		// same but request 9h worth of data
		// raw 32400/60 -> 540. chosen for same reason
		{
			[]models.Req{
				reqRaw("a", 0, 3600*9, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*9, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*9, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*9, 1000, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 3600*9, 1000, 30, consolidation.Avg, 0, 30, 60, 2),
				reqOut("c", 0, 3600*9, 1000, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},
		// same but request 24h worth of data
		// raw 86400/60 -> 1440
		// agg1 86400/600 -> 144 points -> best choice
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24, 1000, 10, consolidation.Avg, 1, 600, 600, 1),
				reqOut("b", 0, 3600*24, 1000, 30, consolidation.Avg, 1, 600, 600, 1),
				reqOut("c", 0, 3600*24, 1000, 60, consolidation.Avg, 1, 600, 600, 1),
			},
			nil,
		},
		// same but now let's request 2 weeks worth of data.
		// not using raw is a no brainer.
		// agg1 3600*24*7 / 600 = 1008 points, which is too many, so must also do runtime consolidation and bring it back to 504
		// agg2 3600*24*7 / 7200 = 84 points -> too far below maxdatapoints, better to do agg1 with runtime consol
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*7, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24*7, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*7, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*7, 1000, 10, consolidation.Avg, 1, 600, 1200, 2),
				reqOut("b", 0, 3600*24*7, 1000, 30, consolidation.Avg, 1, 600, 1200, 2),
				reqOut("c", 0, 3600*24*7, 1000, 60, consolidation.Avg, 1, 600, 1200, 2),
			},
			nil,
		},
		// let's request 1 year of data
		// raw 3600*24*365/60 -> 525600
		// agg1 3600*24*365/600 -> 52560
		// agg2 3600*24*365/7200 -> 4380
		// agg3 3600*24*365/21600 -> 1460
		// clearly agg3 is the best, and we have to runtime consolidate with aggNum 2
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*365, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*365, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*365, 1000, 10, consolidation.Avg, 3, 21600, 43200, 2),
				reqOut("b", 0, 3600*24*365, 1000, 30, consolidation.Avg, 3, 21600, 43200, 2),
				reqOut("c", 0, 3600*24*365, 1000, 60, consolidation.Avg, 3, 21600, 43200, 2),
			},
			nil,
		},
		// ditto but disable agg3
		// so we have to use agg2 with aggNum of 5
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*365, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*365, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, false),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*365, 1000, 10, consolidation.Avg, 2, 7200, 36000, 5),
				reqOut("b", 0, 3600*24*365, 1000, 30, consolidation.Avg, 2, 7200, 36000, 5),
				reqOut("c", 0, 3600*24*365, 1000, 60, consolidation.Avg, 2, 7200, 36000, 5),
			},
			nil,
		},
		// now let's request 1 year of data again, but without actually having any aggregation bands (wowa don't do this)
		// raw 3600*24*365/60 -> 525600
		// we need an aggNum of 526 to keep this under 1000 points
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*365, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*365, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*365, 1000, 10, consolidation.Avg, 0, 10, 31560, 526*6),
				reqOut("b", 0, 3600*24*365, 1000, 30, consolidation.Avg, 0, 30, 31560, 526*2),
				reqOut("c", 0, 3600*24*365, 1000, 60, consolidation.Avg, 0, 60, 31560, 526),
			},
			nil,
		},
		// same thing but if the metrics have the same resolution
		// raw 3600*24*365/60 -> 525600
		// we need an aggNum of 526 to keep this under 1000 points
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("b", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*365, 1000, 30, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*365, 1000, 30, consolidation.Avg, 0, 30, 31560, 526*2),
				reqOut("b", 0, 3600*24*365, 1000, 30, consolidation.Avg, 0, 30, 31560, 526*2),
				reqOut("c", 0, 3600*24*365, 1000, 30, consolidation.Avg, 0, 30, 31560, 526*2),
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
			t.Errorf("different number of requests for testcase %d  expected: %v, got: %v", i, len(ac.outReqs), len(out))
		} else {
			for r, exp := range ac.outReqs {
				if !compareReqEqual(exp, out[r]) {
					t.Errorf("testcase %d, request %d:\nexpected: %v\n     got: %v", i, r, exp.DebugString(), out[r].DebugString())
				}
			}
		}
	}
}

// return true if a and b are equal. Equal means that all of the struct
// fields are equal.  For the req.Node, we just compare the node.Name rather
// then doing a deep comparision.
func compareReqEqual(a, b models.Req) bool {
	if a.Key != b.Key {
		return false
	}
	if a.Target != b.Target {
		return false
	}
	if a.From != b.From {
		return false
	}
	if a.To != b.To {
		return false
	}
	if a.MaxPoints != b.MaxPoints {
		return false
	}
	if a.RawInterval != b.RawInterval {
		return false
	}
	if a.Consolidator != b.Consolidator {
		return false
	}
	if a.Node.Name != b.Node.Name {
		return false
	}
	if a.Archive != b.Archive {
		return false
	}
	if a.ArchInterval != b.ArchInterval {
		return false
	}
	if a.OutInterval != b.OutInterval {
		return false
	}
	if a.AggNum != b.AggNum {
		return false
	}
	return true
}

func TestMergeSeries(t *testing.T) {
	out := make([]models.Series, 0)
	for i := 0; i < 5; i++ {
		out = append(out, models.Series{
			Target: fmt.Sprintf("some.series.foo%d", i),
			Datapoints: []schema.Point{
				{Val: math.NaN(), Ts: 1449178131},
				{Val: math.NaN(), Ts: 1449178141},
				{Val: 3, Ts: 1449178151},
				{Val: 4, Ts: 1449178161},
			},
			Interval: 10,
		})
	}
	out = append(out, models.Series{
		Target: "some.series.foo1",
		Datapoints: []schema.Point{
			{Val: 1, Ts: 1449178131},
			{Val: 2, Ts: 1449178141},
			{Val: math.NaN(), Ts: 1449178151},
			{Val: math.NaN(), Ts: 1449178161},
		},
		Interval: 10,
	})

	merged := mergeSeries(out)
	if len(merged) != 5 {
		t.Errorf("Expected data to be merged down to 5 series. got %d instead", len(merged))
	}
	for _, serie := range merged {
		if serie.Target == "some.series.foo1" {
			if len(serie.Datapoints) != 4 {
				t.Errorf("expected 4 datapoints. got %d", len(serie.Datapoints))
			}
			for _, pt := range serie.Datapoints {
				if math.IsNaN(pt.Val) {
					t.Errorf("merging should have removed NaN values.")
				}
			}

		}
	}
}

func TestRequestContextWithoutConsolidator(t *testing.T) {
	metric := "metric1"
	archInterval := uint32(10)
	req := reqRaw(metric, 44, 88, 100, 10, consolidation.None)
	req.ArchInterval = archInterval
	ctx := newRequestContext(&req, consolidation.None)

	expectFrom := uint32(41)
	if ctx.From != expectFrom {
		t.Errorf("requestContext.From is not at boundary as expected, expected/got %d/%d", expectFrom, ctx.From)
	}

	expectTo := uint32(81)
	if ctx.To != expectTo {
		t.Errorf("requestContext.To is not at boundary as expected, expected/got %d/%d", expectTo, ctx.To)
	}

	expectedAggKey := ""
	if ctx.AggKey != expectedAggKey {
		t.Errorf("requestContext.AggKey is not empty as expected, expected/got \"%s\"/\"%s\"", expectedAggKey, ctx.AggKey)
	}
}

func TestRequestContextWithConsolidator(t *testing.T) {
	metric := "metric1"
	archInterval := uint32(10)
	from := uint32(44)
	to := uint32(88)
	req := reqRaw(metric, from, to, 100, 10, consolidation.Sum)
	req.ArchInterval = archInterval
	ctx := newRequestContext(&req, consolidation.Sum)

	expectFrom := from
	if ctx.From != expectFrom {
		t.Errorf("requestContext.From is not original value as expected, expected/got %d/%d", expectFrom, ctx.From)
	}

	expectTo := to
	if ctx.To != expectTo {
		t.Errorf("requestContext.To is not original value as expected, expected/got %d/%d", expectTo, ctx.To)
	}

	expectedAggKey := fmt.Sprintf("%s_%s_%d", metric, "sum", archInterval)
	if ctx.AggKey != expectedAggKey {
		t.Errorf("requestContext.AggKey is not as expected, expected/got \"%s\"/\"%s\"", expectedAggKey, ctx.AggKey)
	}
}

// generates and returns a slice of chunks according to specified specs
func generateChunks(span uint32, start uint32, end uint32) []chunk.Chunk {
	var chunks []chunk.Chunk

	c := chunk.New(start)
	for i := start; i < end; i++ {
		c.Push(i, float64((i-start)*2))
		if (i+1)%span == 0 {
			// Mark the chunk that just got finished as finished
			c.Finish()
			chunks = append(chunks, *c)
			if i < end {
				c = chunk.New(i + 1)
			}
		}
	}
	return chunks
}

// the idea of this test is that we want to put some chunks into the
// cache and some into the backend store and some into both,
// then we call getSeriesCachedStore and check if it has stitched the pieces
// from the different sources together correctly.
// additionally we check the chunk cache's hit statistics to verify that it
// has been used correctly.
//
// cache:                             |-----|-----|
// store:           |-----|-----|-----|-----|
//
// query:              |---------------|
// result:          |-----|-----|-----|-----|
//
// query:                                |--------|
// result:                            |-----|-----|
//
func TestGetSeriesCachedStore(t *testing.T) {
	span := uint32(600)
	// save some electrons by skipping steps that are no edge cases
	steps := span / 10
	start := span
	// we want 10 chunks to serve the largest testcase
	end := span * 11
	chunks := generateChunks(span, start, end)

	srv, _ := NewServer()
	store := mdata.NewMockStore()
	srv.BindBackendStore(store)
	metrics := mdata.NewAggMetrics(store, &cache.MockCache{}, 1, 1, 0, 0, 0, 0, []mdata.AggSetting{})
	srv.BindMemoryStore(metrics)
	metric := "metric1"
	var c *cache.CCache
	var itgen *chunk.IterGen
	var prevts uint32

	type testcase struct {
		// the pattern of chunks in store, cache or both
		Pattern string

		// expected number of cache hits on query over all chunks
		// used to verify that cache is used when it should be used
		Hits uint32
	}

	testcases := []testcase{
		{"sscc", 2},
		{"ssbb", 2},
		{"ccss", 2},
		{"bbss", 2},
		{"ccsscc", 4},
		{"bbssbb", 4},
		{"ssbbss", 0},     // cannot use cache in the middle of the queried range
		{"ssbbssbbss", 0}, // cannot use cache in the middle of the queried range. this one mimics what happens when somebody has a dashboard showing the first week of the last few months
	}

	// going through all testcases
	for _, tc := range testcases {
		pattern := tc.Pattern

		// last ts is start ts plus the number of spans the pattern defines
		lastTs := start + span*uint32(len(pattern))

		// we want to query through various ranges, including:
		// - from first ts to first ts
		// - from first ts to last ts
		// - from last ts to last ts
		// and various ranges between
		for from := start; from <= lastTs; from += steps {
			for to := from; to <= lastTs; to += steps {
				// reinstantiate the cache at the beginning of each run
				c = cache.NewCCache()
				srv.BindCache(c)

				// populate cache and store according to pattern definition
				prevts = 0
				for i := 0; i < len(tc.Pattern); i++ {
					itgen = chunk.NewBareIterGen(chunks[i].Series.Bytes(), chunks[i].Series.T0, span)
					if pattern[i] == 'c' || pattern[i] == 'b' {
						c.Add(metric, prevts, *itgen)
					}
					if pattern[i] == 's' || pattern[i] == 'b' {
						store.AddMockResult(metric, *itgen)
					}
					prevts = chunks[i].T0
				}

				// create a request for the current range
				req := reqRaw(metric, from, to, span, 1, consolidation.None)
				req.ArchInterval = 1
				ctx := newRequestContext(&req, consolidation.None)
				iters := srv.getSeriesCachedStore(ctx, to)

				// expecting the first returned timestamp to be the T0 of the chunk containing "from"
				expectResFrom := from - (from % span)

				// expecting the last returned timestamp to be the last point in the chunk containing "to"
				expectResTo := (to - 1) + (span - (to-1)%span) - 1

				// for each timestamp in the returned iterators we compare if it has the expected value
				// we use the tsTracker to increase together with the iterators and compare at each step
				tsTracker := expectResFrom

				tsSlice := make([]uint32, 0)
				for i, it := range iters {
					for it.Next() {
						ts, _ := it.Values()
						if ts != tsTracker {
							t.Fatalf("Pattern %s From %d To %d; expected value %d is %d, but got %d", pattern, from, to, i, tsTracker, ts)
						}
						tsTracker++
						tsSlice = append(tsSlice, ts)
					}
				}

				if to-from > 0 {
					if len(tsSlice) == 0 {
						t.Fatalf("Pattern %s From %d To %d; Should have >0 results but got 0", pattern, from, to)
					}
					if tsSlice[0] != expectResFrom {
						t.Fatalf("Pattern %s From %d To %d; Expected first to be %d but got %d", pattern, from, to, expectResFrom, tsSlice[0])
					}
					if tsSlice[len(tsSlice)-1] != expectResTo {
						t.Fatalf("Pattern %s From %d To %d; Expected last to be %d but got %d", pattern, from, to, expectResTo, tsSlice[len(tsSlice)-1])
					}
				} else if len(tsSlice) > 0 {
					t.Fatalf("Pattern %s From %d To %d; Expected results to have len 0 but got %d", pattern, from, to, len(tsSlice))
				}

				expectedHits := uint32(0)
				complete := false
				// because ranges are exclusive at the end we'll test for to - 1
				exclTo := to - 1

				// if from is equal to we always expect 0 hits
				if from != to {

					// seek hits from beginning of the searched ranged within the given pattern
					for i := 0; i < len(pattern); i++ {

						// if pattern index is lower than from's chunk we continue
						if from-(from%span) > start+uint32(i)*span {
							continue
						}

						// current pattern index is a cache hit, so we expect one more
						if pattern[i] == 'c' || pattern[i] == 'b' {
							expectedHits++
						} else {
							break
						}

						// if we've already seeked beyond to's pattern we break and mark the seek as complete
						if exclTo-(exclTo%span) == start+uint32(i)*span {
							complete = true
							break
						}
					}

					// only if the previous seek was not complete we launch one from the other end
					if !complete {

						// now the same from the other end (just like the cache searching does)
						for i := len(pattern) - 1; i >= 0; i-- {

							// if pattern index is above to's chunk we continue
							if exclTo-(exclTo%span)+span <= start+uint32(i)*span {
								continue
							}

							// current pattern index is a cache hit, so we expecte one more
							if pattern[i] == 'c' || pattern[i] == 'b' {
								expectedHits++
							} else {
								break
							}

							// if we've already seeked beyond from's pattern we break
							if from-(from%span) == start+uint32(i)*span {
								break
							}
						}
					}
				}

				// verify we got all cache hits we should have
				hits := accnt.CacheChunkHit.Peek()
				if expectedHits != hits {
					t.Fatalf("Pattern %s From %d To %d; Expected %d hits but got %d", pattern, from, to, expectedHits, hits)
				}
				accnt.CacheChunkHit.SetUint32(0)

				// stop cache go routines before reinstantiating it at the top of the loop
				c.Stop()
				store.ResetMock()
			}
		}
	}
}

func TestGetSeriesAggMetrics(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	store := mdata.NewMockStore()
	chunkSpan := uint32(600)
	numChunks := uint32(10)
	metrics := mdata.NewAggMetrics(store, &cache.MockCache{}, chunkSpan, numChunks, 0, 0, 0, 0, []mdata.AggSetting{})
	srv, _ := NewServer()
	srv.BindBackendStore(store)
	srv.BindMemoryStore(metrics)
	from := uint32(1744)
	to := uint32(1888)
	metricKey := "metric1"
	archInterval := uint32(10)
	req := reqRaw(metricKey, from, to, 100, 10, consolidation.None)
	req.ArchInterval = archInterval
	ctx := newRequestContext(&req, consolidation.None)

	metric := metrics.GetOrCreate(metricKey)
	for i := uint32(50); i < 3000; i++ {
		metric.Add(i, float64(i^2))
	}

	oldest, iters := srv.getSeriesAggMetrics(ctx)
	timestamps := make([]uint32, 0)
	values := make([]float64, 0)

	for _, it := range iters {
		for it.Next() {
			ts, val := it.Values()
			timestamps = append(timestamps, ts)
			values = append(values, val)
		}
	}

	// should be the T0 of the chunk from (1744) is in
	// 1744 - (1744 % 600) = 1200
	expected := uint32(1200)
	if oldest != expected {
		t.Errorf("Expected oldest to be %d but got %d", expected, oldest)
	}

	// number of returned ts should be the number of chunks the searched range spans across * chunkspan
	// chunk that contains from starts at (1744 - (1744 % 600)) = 1200
	// chunk that contains to starts at (1888 + (600 - 1888 % 600)) = 1800
	// so we expect two chunks, which means 2*chunkspan = 1200
	expected = uint32(1200)
	if uint32(len(timestamps)) != expected {
		t.Errorf("Returned timestamps are not right, should have %d but got %d",
			to-from,
			len(timestamps),
		)
	}

	// should be the beginning of the chunk containing from
	// 1744 - (1744 % 600)
	expected = 1200
	if timestamps[0] != expected {
		t.Errorf("First timestamp is not right, expected %d but got %d", expected, timestamps[0])
	}

	// should be the beginning of chunk containing to plus chunk span - 1 (exclusive)
	// 1888 - (1888 % 600) + 600 - 1 = 1799
	expected = uint32(2399)
	if timestamps[len(timestamps)-1] != expected {
		t.Errorf("Last timestamp is not right, expected %d but got %d", expected, timestamps[len(timestamps)-1])
	}
}

// these serve as a "cache" of clean points we can use instead of regenerating all the time
var randFloatsWithNullsBuf []schema.Point
var randFloatsBuf []schema.Point

func randFloats() []schema.Point {
	if len(randFloatsBuf) == 0 {
		// let's just do the "odd" case, since the non-odd will be sufficiently close
		randFloatsBuf = make([]schema.Point, 1000001)
		for i := 0; i < len(randFloatsBuf); i++ {
			randFloatsBuf[i] = schema.Point{Val: rand.Float64(), Ts: uint32(i)}
		}
	}
	out := make([]schema.Point, len(randFloatsBuf))
	copy(out, randFloatsBuf)
	return out
}

func randFloatsWithNulls() []schema.Point {
	if len(randFloatsWithNullsBuf) == 0 {
		// let's just do the "odd" case, since the non-odd will be sufficiently close
		randFloatsWithNullsBuf = make([]schema.Point, 1000001)
		for i := 0; i < len(randFloatsWithNullsBuf); i++ {
			if i%2 == 0 {
				randFloatsWithNullsBuf[i] = schema.Point{Val: math.NaN(), Ts: uint32(i)}
			} else {
				randFloatsWithNullsBuf[i] = schema.Point{Val: rand.Float64(), Ts: uint32(i)}
			}
		}
	}
	out := make([]schema.Point, len(randFloatsBuf))
	copy(out, randFloatsWithNullsBuf)
	return out
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
		out := Fix(in, 0, 1000001, 1)
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

var result []models.Req

func BenchmarkAlignRequests(b *testing.B) {
	var res []models.Req
	reqs := []models.Req{
		reqRaw("a", 0, 3600*24*7, 1000, 10, consolidation.Avg),
		reqRaw("b", 0, 3600*24*7, 1000, 30, consolidation.Avg),
		reqRaw("c", 0, 3600*24*7, 1000, 60, consolidation.Avg),
	}
	aggSettings := mdata.AggSettings{
		35 * 24 * 60 * 60,
		[]mdata.AggSetting{
			mdata.NewAggSetting(600, 21600, 1, 0, true),
			mdata.NewAggSetting(7200, 21600, 1, 0, true),
			mdata.NewAggSetting(21600, 21600, 1, 0, true),
		},
	}

	for n := 0; n < b.N; n++ {
		res, _ = alignRequests(reqs, aggSettings)
	}
	result = res
}
