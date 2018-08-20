package api

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/cache"
	"github.com/grafana/metrictank/mdata/cache/accnt"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
)

func init() {
	cluster.Init("default", "test", time.Now(), "http", 6060)
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
// for each case, we generate a new series of 5 points to cover every possible combination of:
// * every possible data   offset (against its quantized version)       e.g. offset between 0 and interval-1
// * every possible `from` offset (against its quantized query results) e.g. offset between 0 and interval-1
// * every possible `to`   offset (against its quantized query results) e.g. offset between 0 and interval-1
// and asserts that we get the appropriate data back in all possible query (based on to/from) of the raw data

func TestGetSeriesFixed(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	store := mdata.NewMockStore()
	store.Drop = true

	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)
	mdata.SetSingleSchema(conf.NewRetentionMT(10, 100, 600, 10, true))

	metrics := mdata.NewAggMetrics(store, &cache.MockCache{}, false, 0, 0, 0)
	srv, _ := NewServer()
	srv.BindBackendStore(store)
	srv.BindMemoryStore(metrics)

	expected := []schema.Point{
		{Val: 20, Ts: 20},
		{Val: 30, Ts: 30},
	}

	var num int

	for offset := uint32(1); offset <= 10; offset++ {
		for from := uint32(11); from <= 20; from++ { // should always yield result with first point at 20 (because from is inclusive)
			for to := uint32(31); to <= 40; to++ { // should always yield result with last point at 30 (because to is exclusive)
				num += 1
				id := test.GetMKey(num)

				metric := metrics.GetOrCreate(id, 0, 0)
				metric.Add(offset, 10)    // this point will always be quantized to 10
				metric.Add(10+offset, 20) // this point will always be quantized to 20, so it should be selected
				metric.Add(20+offset, 30) // this point will always be quantized to 30, so it should be selected
				metric.Add(30+offset, 40) // this point will always be quantized to 40
				metric.Add(40+offset, 50) // this point will always be quantized to 50
				req := models.NewReq(id, "", "", from, to, 1000, 10, consolidation.Avg, 0, cluster.Manager.ThisNode(), 0, 0)
				req.ArchInterval = 10
				points, err := srv.getSeriesFixed(test.NewContext(), req, consolidation.None)
				if err != nil {
					t.Errorf("case %d: offset %d, from %d to %d -> error: %s", num, offset, from, to, err)
				}
				if !reflect.DeepEqual(expected, points) {
					t.Errorf("case %d: offset %d, from %d to %d -> exp: %v - got %v", num, offset, from, to, expected, points)
				}
			}
		}
	}
}

func reqRaw(key schema.MKey, from, to, maxPoints, rawInterval uint32, consolidator consolidation.Consolidator, schemaId, aggId uint16) models.Req {
	req := models.NewReq(key, "", "", from, to, maxPoints, rawInterval, consolidator, 0, cluster.Manager.ThisNode(), schemaId, aggId)
	return req
}
func reqOut(key schema.MKey, from, to, maxPoints, rawInterval uint32, consolidator consolidation.Consolidator, schemaId, aggId uint16, archive int, archInterval, ttl, outInterval, aggNum uint32) models.Req {
	req := models.NewReq(key, "", "", from, to, maxPoints, rawInterval, consolidator, 0, cluster.Manager.ThisNode(), schemaId, aggId)
	req.Archive = archive
	req.ArchInterval = archInterval
	req.TTL = ttl
	req.OutInterval = outInterval
	req.AggNum = aggNum
	return req
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

// generates and returns a slice of chunks according to specified specs
func generateChunks(span uint32, start uint32, end uint32) []chunk.Chunk {
	var chunks []chunk.Chunk

	c := chunk.New(start)
	for ts := start; ts < end; ts++ {
		val := float64((ts - start) * 2)
		c.Push(ts, val)
		// handle the case of this being the last point for this chunk
		if (ts+1)%span == 0 {
			c.Finish()
			chunks = append(chunks, *c)
			// if there will be a next iteration, prepare the chunk
			if ts+1 < end {
				c = chunk.New(ts + 1)
			}
		}
	}
	// if end was not quantized we have to finish the last chunk
	if !c.Closed {
		c.Finish()
		chunks = append(chunks, *c)
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
	// reduce overhead of creating accounter over and over
	old := accnt.EventQSize
	accnt.EventQSize = 1000
	defer func() { accnt.EventQSize = old }()

	span := uint32(600)
	start := span
	// we want 10 chunks to serve the largest testcase
	// they will have t0 600, 1200, ..., 5400, 6000
	end := span * 11
	chunks := generateChunks(span, start, end)

	srv, _ := NewServer()
	store := mdata.NewMockStore()
	srv.BindBackendStore(store)

	metrics := mdata.NewAggMetrics(store, &cache.MockCache{}, false, 0, 0, 0)
	srv.BindMemoryStore(metrics)
	metric := test.GetAMKey(1)

	type testcase struct {
		// the pattern of chunks
		// c: in cache
		// s: in store
		// b: in both
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

		// lastTs is the t0 of the first chunk that comes after the used range
		lastTs := start + span*uint32(len(pattern))

		// we want to query through various ranges, including:
		// - from first ts to first ts
		// - from first ts to last ts
		// - from last ts to last ts
		// and various ranges between
		// we increment from and to in thirds of a span,
		// because incrementing by 1 would be needlessly expensive
		step := span / 3
		for from := start; from <= lastTs; from += step {
			for to := from; to <= lastTs; to += step {
				// use fresh store and cache
				c := cache.NewCCache()
				srv.BindCache(c)
				store.Reset()

				// populate cache and store according to pattern definition
				var prevts uint32
				for i := 0; i < len(tc.Pattern); i++ {
					itgen := chunk.NewBareIterGen(chunks[i].Series.Bytes(), chunks[i].Series.T0, span)
					if pattern[i] == 'c' || pattern[i] == 'b' {
						c.Add(metric, prevts, *itgen)
					}
					if pattern[i] == 's' || pattern[i] == 'b' {
						cwr := mdata.NewChunkWriteRequest(nil, metric, &chunks[i], 0, span, time.Now())
						store.Add(&cwr)
					}
					prevts = chunks[i].T0
				}

				// create a request for the current range
				req := reqRaw(metric.MKey, from, to, span, 1, consolidation.None, 0, 0)
				req.ArchInterval = 1
				ctx := newRequestContext(test.NewContext(), &req, consolidation.None)
				iters, err := srv.getSeriesCachedStore(ctx, to)

				// test invalid query; from must be less than to
				if from == to {
					if err == nil {
						t.Fatalf("Pattern %s From=To %d: expected err, got nil", pattern, from)
					}
					continue
				}
				if err != nil {
					t.Fatalf("Pattern %s From %d To %d: error %s", pattern, from, to, err)
				}

				// expecting the first returned timestamp to be the T0 of the chunk containing "from"
				expectResFrom := from - (from % span)

				// expecting the last returned timestamp to be the last point in the chunk containing "to"
				expectResTo := (to - 1) + (span - (to-1)%span) - 1

				// for each timestamp in the returned iterators we compare if it has the expected value
				// we use the tsTracker to increase together with the iterators and compare at each step
				tsTracker := expectResFrom

				var tsSlice []uint32
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

				if len(tsSlice) == 0 {
					t.Fatalf("Pattern %s From %d To %d; Should have >0 results but got 0", pattern, from, to)
				}
				if tsSlice[0] != expectResFrom {
					t.Fatalf("Pattern %s From %d To %d; Expected first to be %d but got %d", pattern, from, to, expectResFrom, tsSlice[0])
				}
				if tsSlice[len(tsSlice)-1] != expectResTo {
					t.Fatalf("Pattern %s From %d To %d; Expected last to be %d but got %d", pattern, from, to, expectResTo, tsSlice[len(tsSlice)-1])
				}

				expectedHits := uint32(0)
				complete := false
				// because ranges are exclusive at the end we'll test for to - 1
				exclTo := to - 1

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

				// verify we got all cache hits we should have
				hits := accnt.CacheChunkHit.Peek()
				if expectedHits != hits {
					t.Fatalf("Pattern %s From %d To %d; Expected %d hits but got %d", pattern, from, to, expectedHits, hits)
				}
				accnt.CacheChunkHit.SetUint32(0)

				// stop cache go routines before reinstantiating it at the top of the loop
				c.Stop()
			}
		}
	}
}

func TestGetSeriesAggMetrics(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	store := mdata.NewMockStore()

	metrics := mdata.NewAggMetrics(store, &cache.MockCache{}, false, 0, 0, 0)
	srv, _ := NewServer()
	srv.BindBackendStore(store)
	srv.BindMemoryStore(metrics)
	from := uint32(1744)
	to := uint32(1888)
	metricKey := test.GetMKey(1)
	archInterval := uint32(10)
	req := reqRaw(metricKey, from, to, 100, 10, consolidation.None, 0, 0)
	req.ArchInterval = archInterval
	ctx := newRequestContext(test.NewContext(), &req, consolidation.None)

	metric := metrics.GetOrCreate(metricKey, 0, 0)
	for i := uint32(50); i < 3000; i++ {
		metric.Add(i, float64(i^2))
	}

	res, err := srv.getSeriesAggMetrics(ctx)
	if err != nil {
		t.Errorf("Got unexpected error %q", err)
	}
	timestamps := make([]uint32, 0)
	values := make([]float64, 0)

	for _, it := range res.Iters {
		for it.Next() {
			ts, val := it.Values()
			timestamps = append(timestamps, ts)
			values = append(values, val)
		}
	}

	// should be the T0 of the chunk from (1744) is in
	// 1744 - (1744 % 600) = 1200
	expected := uint32(1200)
	if res.Oldest != expected {
		t.Errorf("Expected oldest to be %d but got %d", expected, res.Oldest)
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

var dummy []schema.Point

func BenchmarkFix1M(b *testing.B) {
	var l int
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		in := test.RandFloats1M()
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
		ina := test.RandFloats1M()
		inb := test.RandFloats1M()
		l = len(ina)
		b.StartTimer()
		out := divide(ina, inb)
		dummy = out
	}
	b.SetBytes(int64(l * 12))
}
