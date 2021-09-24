package mdata

import (
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/schema"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/mdata/cache"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/mdata/chunk/tsz"
	"github.com/grafana/metrictank/test"
)

var mockstore = NewMockStore()

type MockFlusher struct {
	mockstore *MockStore
	mockCache *cache.MockCache
}

func (m MockFlusher) Cache(metric schema.AMKey, prev uint32, itergen chunk.IterGen) {
	if m.mockCache != nil {
		m.mockCache.AddIfHot(metric, prev, itergen)
	}
}

func (m MockFlusher) Store(cwr *ChunkWriteRequest) {
	m.mockstore.Add(cwr)
}

type point struct {
	ts  uint32
	val float64
}

type ByTs []point

func (a ByTs) Len() int           { return len(a) }
func (a ByTs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTs) Less(i, j int) bool { return a[i].ts < a[j].ts }

func (p point) String() string {
	return fmt.Sprintf("point{%0.f at %d}", p.val, p.ts)
}

type Checker struct {
	t      *testing.T
	agg    *AggMetric
	points []point
}

func NewChecker(t *testing.T, agg *AggMetric) *Checker {
	return &Checker{t, agg, make([]point, 0)}
}

func (c *Checker) Add(ts uint32, val float64) {
	c.agg.Add(ts, val)
	c.points = append(c.points, point{ts, val})
}

func (c *Checker) DropPointByTs(ts uint32) {
	for i := 0; i != len(c.points); {
		if c.points[i].ts == ts {
			c.points = append(c.points[:i], c.points[i+1:]...)
		} else {
			i++
		}
	}
}

// from to is the range that gets requested from AggMetric
// first/last is what we use as data range to compare to (both inclusive)
// these may be different because AggMetric returns broader rangers (due to packed format),
func (c *Checker) Verify(primary bool, from, to, first, last uint32) {
	currentClusterStatus := cluster.Manager.IsPrimary()
	sort.Sort(ByTs(c.points))
	cluster.Manager.SetPrimary(primary)
	res, err := c.agg.Get(from, to)

	if err != nil {
		c.t.Fatalf("expected err nil, got %v", err)
	}

	// we don't do checking or fancy logic, it is assumed that the caller made sure first and last are ts of actual points
	var pi int // index of first point we want
	var pj int // index of last point we want
	for pi = 0; c.points[pi].ts != first; pi++ {
	}
	for pj = pi; c.points[pj].ts != last; pj++ {
	}
	c.t.Logf("verifying AggMetric.Get(%d,%d) -> range is %d - %d ?", from, to, first, last)
	index := pi - 1
	for _, iter := range res.Iters {
		for iter.Next() {
			index++
			tt, vv := iter.Values()
			if index > pj {
				c.t.Fatalf("Iters: Values()=(%v,%v), want end of stream\n", tt, vv)
			}
			if c.points[index].ts != tt || c.points[index].val != vv {
				c.t.Fatalf("Iters: Values()=(%v,%v), want (%v,%v)\n", tt, vv, c.points[index].ts, c.points[index].val)
			}
		}
	}
	for _, point := range res.Points {
		index++
		if index > pj {
			c.t.Fatalf("Points: Values()=(%v,%v), want end of stream\n", point.Ts, point.Val)
		}
		if c.points[index].ts != point.Ts || c.points[index].val != point.Val {
			c.t.Fatalf("Points: Values()=(%v,%v), want (%v,%v)\n", point.Ts, point.Val, c.points[index].ts, c.points[index].val)
		}
	}
	if index != pj {
		c.t.Fatalf("not all values returned. missing %v", c.points[index:pj+1])
	}
	cluster.Manager.SetPrimary(currentClusterStatus)
}

func TestMetricPersistBeingPrimary(t *testing.T) {
	testMetricPersistOptionalPrimary(t, true)
}

func TestMetricPersistBeingSecondary(t *testing.T) {
	testMetricPersistOptionalPrimary(t, false)
}

func testMetricPersistOptionalPrimary(t *testing.T, primary bool) {
	// always reset the counter when entering and leaving the test
	mockstore.Reset()
	defer mockstore.Reset()

	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(primary)

	callCount := uint32(0)
	calledCb := make(chan bool)

	mockCache := cache.MockCache{}
	mockCache.AddIfHotCb = func() { calledCb <- true }

	chunkAddCount, chunkSpan := uint32(10), uint32(300)
	rets := conf.MustParseRetentions("1s:1s:5min:5:true")
	agg := NewAggMetric(MockFlusher{mockstore, &mockCache}, test.GetAMKey(42), rets, 0, chunkSpan, nil, false, false, 0)

	for ts := chunkSpan; ts <= chunkSpan*chunkAddCount; ts += chunkSpan {
		agg.Add(ts, 1)
	}

	timeout := time.After(1 * time.Second)

	for i := uint32(0); i < chunkAddCount-1; i++ {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting for a callback call")
		case <-calledCb:
			callCount = callCount + 1
		}
	}

	if callCount < chunkAddCount-1 {
		t.Fatalf("there should have been %d chunk pushes, but go %d", chunkAddCount-1, callCount)
	}

	if primary {
		if uint32(mockstore.Items()) != chunkAddCount-1 {
			t.Fatalf("there should have been %d chunk adds on store, but got %d", chunkAddCount-1, mockstore.Items())
		}
	} else {
		if mockstore.Items() != 0 {
			t.Fatalf("there should have been %d chunk adds on store, but go %d", 0, mockstore.Items())
		}
	}
}

func TestAggMetric(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	ret := conf.MustParseRetentions("1s:1s:2min:5:true")
	c := NewChecker(t, NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), ret, 0, 1, nil, false, false, 0))

	// chunk t0's: 120, 240, 360, 480, 600, 720, 840, 960

	// basic case, single range
	c.Add(121, 121)
	c.Verify(true, 120, 240, 121, 121)
	c.Add(125, 125)
	c.Verify(true, 120, 239, 121, 125)
	c.Add(135, 135)
	c.Add(145, 145)
	c.Add(155, 155)
	c.Verify(true, 120, 239, 121, 155)

	// add new ranges, aligned and unaligned
	c.Add(240, 240)
	c.Add(375, 375)
	c.Verify(true, 120, 479, 121, 375)
	c.Verify(false, 120, 479, 121, 375)

	// get subranges
	c.Verify(true, 140, 359, 121, 240)
	c.Verify(true, 260, 359, 240, 240)
	c.Verify(true, 372, 390, 375, 375)

	// border dancing. good for testing inclusivity and exclusivity
	c.Verify(true, 120, 199, 121, 155)
	c.Verify(true, 120, 240, 121, 155)
	c.Verify(true, 120, 241, 121, 240)
	c.Verify(true, 238, 239, 121, 155)
	c.Verify(true, 239, 240, 121, 155)
	c.Verify(true, 240, 241, 240, 240)
	c.Verify(true, 241, 242, 240, 240)
	c.Verify(true, 359, 360, 240, 240)
	c.Verify(true, 360, 361, 375, 375)

	// skipping
	c.Add(610, 610)
	c.Add(612, 612)
	c.Verify(true, 120, 719, 121, 612)

	// basic wraparound
	c.Add(730, 730)
	c.Add(732, 732)
	c.Add(850, 850)
	c.Add(852, 852)
	// TODO would be nice to test that it panics when requesting old range. something with recover?
	//c.Verify(true, 120, 959, 121, 612)

	// largest range we have so far
	c.Verify(true, 360, 959, 375, 852)
	// a smaller range
	c.Verify(true, 602, 959, 610, 852)

	// the circular buffer had these ranges:
	// 120 240 360 skipped 600
	// then we made it:
	// 720 840 360 skipped 600
	// now we want to do another wrap around with skip (must have cleared old data)
	// let's jump to 12*120=1440. the accessible range should then be 960-1440
	// clea 1440 clea clea clea
	// we can't (and shouldn't, due to abstraction) test the clearing itself
	// but we just check we only get this point right before 13*120
	c.Add(1559, 1559)
	// TODO: implement skips and enable this
	//	c.Verify(true, 960, 1559, 1559, 1559)
}

func TestAggMetricWithReorderBuffer(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	agg := conf.Aggregation{
		Name:              "Default",
		Pattern:           regexp.MustCompile(".*"),
		XFilesFactor:      0.5,
		AggregationMethod: []conf.Method{conf.Avg},
	}
	ret := conf.MustParseRetentions("1s:1s:2min:5:true")
	c := NewChecker(t, NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), ret, 10, 1, &agg, false, false, 0))

	// basic adds and verifies with test data
	c.Add(121, 121)
	c.Verify(true, 120, 240, 121, 121)
	c.Add(125, 125)
	c.Verify(true, 120, 239, 121, 125)
	c.Add(135, 135)
	c.Add(145, 145)
	c.Add(155, 155)
	c.Verify(true, 120, 239, 121, 155)
	c.Add(240, 240)
	c.Add(375, 375)
	c.Verify(true, 120, 479, 121, 375)

	discardedSampleOutOfOrder.SetUint32(0)

	// adds 10 entries that are out of order and the reorder buffer should order the first 9
	// the last item (365) will be too old, so it increases discardedSampleOutOfOrder counter
	for i := uint32(374); i > 364; i-- {
		c.Add(i, float64(i))
	}
	c.DropPointByTs(365)

	// get subranges
	c.Verify(true, 120, 380, 121, 375)

	// one point has been added out of order and too old for the buffer to reorder
	if discardedSampleOutOfOrder.Peek() != 1 {
		t.Fatalf("Expected the out of order count to be 1, not %d", discardedSampleOutOfOrder.Peek())
	}
}

func TestAggMetricDropFirstChunk(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(true)
	mockstore.Reset()
	rets := conf.MustParseRetentions("1s:1s:10s:5:true")
	m := NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), rets, 0, 1, nil, false, true, 0)
	m.Add(10, 10)
	m.Add(11, 11)
	m.Add(12, 12)
	m.Add(20, 20)
	m.Add(21, 21)
	m.Add(22, 22)
	m.Add(30, 30)
	m.Add(31, 31)
	m.Add(32, 32)
	// the chunk that point belongs to is not returned by the search because the chunk has
	// not been closed yet which will happen if a point belonging to the following chunk is added
	m.Add(40, 40)
	itgens, err := mockstore.Search(test.NewContext(), test.GetAMKey(42), 0, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(itgens) != 2 || itgens[0].T0 != 20 || itgens[1].T0 != 30 {
		t.Fatalf("expected 2 itgens for chunks 20 and 30 since the first one should be dropped. Got %v", itgens)
	}
}

func TestAggMetricIngestFrom(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(true)
	mockstore.Reset()
	ingestFrom := int64(25)
	ret := conf.MustParseRetentions("1s:1s:10s:5:true")
	m := NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), ret, 0, 1, nil, false, false, ingestFrom)
	m.Add(10, 10)
	m.Add(11, 11)
	m.Add(12, 12)
	m.Add(20, 20)
	m.Add(21, 21)
	m.Add(22, 22)
	m.Add(30, 30)
	m.Add(31, 31)
	m.Add(32, 32)
	// the chunk that point belongs to is not returned by the search because the chunk has
	// not been closed yet which will happen if a point belonging to the following chunk is added
	m.Add(40, 40)
	itgens, err := mockstore.Search(test.NewContext(), test.GetAMKey(42), 0, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(itgens) != 1 || itgens[0].T0 != 30 {
		t.Fatalf("expected 1 itgens for chunk 30 since the chunks before 25 should be dropped. Got %v", itgens)
	}
}

// TestAggMetricFutureTolerance tests whether the future tolerance limit works correctly
// there is a race condition because it depends on the return value of time.Now().Unix(),
// realistically it should never fail due to that race condition unless it executes
// unreasonably slow.
func TestAggMetricFutureTolerance(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(true)
	mockstore.Reset()
	ret := conf.MustParseRetentions("1s:10m:6h:5:true")

	_futureToleranceRatio := futureToleranceRatio
	_enforceFutureTolerance := enforceFutureTolerance
	discardedSampleTooFarAhead.SetUint32(0)
	sampleTooFarAhead.SetUint32(0)
	defer func() {
		futureToleranceRatio = _futureToleranceRatio
		enforceFutureTolerance = _enforceFutureTolerance
		discardedSampleTooFarAhead.SetUint32(0)
		sampleTooFarAhead.SetUint32(0)
	}()

	// with a raw retention of 600s, this will result in a future tolerance of 60s
	futureToleranceRatio = 10
	aggMetricTolerate60 := NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), ret, 0, 1, nil, false, false, 0)

	// will not tolerate future datapoints at all
	futureToleranceRatio = 0
	aggMetricTolerate0 := NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), ret, 0, 1, nil, false, false, 0)

	// add datapoint which is 30 seconds in the future to both aggmetrics, they should both accept it
	// because enforcement of future tolerance is disabled, but the one with tolerance 0 should increase
	// the counter of data points that would have been rejected
	enforceFutureTolerance = false
	aggMetricTolerate60.Add(uint32(time.Now().Unix()+30), 10)
	if len(aggMetricTolerate60.chunks) != 1 {
		t.Fatalf("expected to have 1 chunk in aggmetric, but there were %d", len(aggMetricTolerate60.chunks))
	}
	if sampleTooFarAhead.Peek() != 0 {
		t.Fatalf("expected the sampleTooFarAhead count to be 0, but it was %d", sampleTooFarAhead.Peek())
	}
	if discardedSampleTooFarAhead.Peek() != 0 {
		t.Fatalf("expected the discardedSampleTooFarAhead count to be 0, but it was %d", discardedSampleTooFarAhead.Peek())
	}

	aggMetricTolerate0.Add(uint32(time.Now().Unix()+30), 10)
	if len(aggMetricTolerate0.chunks) != 1 {
		t.Fatalf("expected to have 1 chunk in aggmetric, but there were %d", len(aggMetricTolerate0.chunks))
	}
	if sampleTooFarAhead.Peek() != 1 {
		t.Fatalf("expected the sampleTooFarAhead count to be 1, but it was %d", sampleTooFarAhead.Peek())
	}
	if discardedSampleTooFarAhead.Peek() != 0 {
		t.Fatalf("expected the discardedSampleTooFarAhead count to be 0, but it was %d", discardedSampleTooFarAhead.Peek())
	}

	// enable the enforcement of the future tolerance limit and re-initialize the two agg metrics
	// then add a data point with time stamp 30 sec in the future to both aggmetrics again.
	// this time only the one that tolerates up to 60 secs should accept the datapoint.
	discardedSampleTooFarAhead.SetUint32(0)
	sampleTooFarAhead.SetUint32(0)
	enforceFutureTolerance = true
	futureToleranceRatio = 10
	aggMetricTolerate60 = NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), ret, 0, 1, nil, false, false, 0)
	futureToleranceRatio = 0
	aggMetricTolerate0 = NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), ret, 0, 1, nil, false, false, 0)

	aggMetricTolerate60.Add(uint32(time.Now().Unix()+30), 10)
	if len(aggMetricTolerate60.chunks) != 1 {
		t.Fatalf("expected to have 1 chunk in aggmetric, but there were %d", len(aggMetricTolerate60.chunks))
	}
	if sampleTooFarAhead.Peek() != 0 {
		t.Fatalf("expected the sampleTooFarAhead count to be 0, but it was %d", sampleTooFarAhead.Peek())
	}
	if discardedSampleTooFarAhead.Peek() != 0 {
		t.Fatalf("expected the discardedSampleTooFarAhead count to be 0, but it was %d", discardedSampleTooFarAhead.Peek())
	}

	aggMetricTolerate0.Add(uint32(time.Now().Unix()+30), 10)
	if len(aggMetricTolerate0.chunks) != 0 {
		t.Fatalf("expected to have 0 chunks in aggmetric, but there were %d", len(aggMetricTolerate0.chunks))
	}
	if sampleTooFarAhead.Peek() != 1 {
		t.Fatalf("expected the sampleTooFarAhead count to be 1, but it was %d", sampleTooFarAhead.Peek())
	}
	if discardedSampleTooFarAhead.Peek() != 1 {
		t.Fatalf("expected the discardedSampleTooFarAhead count to be 1, but it was %d", discardedSampleTooFarAhead.Peek())
	}

	// add another datapoint with timestamp of now() to the aggmetric tolerating 0, should be accepted
	discardedSampleTooFarAhead.SetUint32(0)
	sampleTooFarAhead.SetUint32(0)
	aggMetricTolerate0.Add(uint32(time.Now().Unix()), 10)
	if len(aggMetricTolerate0.chunks) != 1 {
		t.Fatalf("expected to have 1 chunk in aggmetric, but there were %d", len(aggMetricTolerate0.chunks))
	}
	if sampleTooFarAhead.Peek() != 0 {
		t.Fatalf("expected the sampleTooFarAhead count to be 0, but it was %d", sampleTooFarAhead.Peek())
	}
	if discardedSampleTooFarAhead.Peek() != 0 {
		t.Fatalf("expected the discardedSampleTooFarAhead count to be 0, but it was %d", discardedSampleTooFarAhead.Peek())
	}
}

func itersToPoints(iters []tsz.Iter) []schema.Point {
	var points []schema.Point
	for _, it := range iters {
		for it.Next() {
			ts, val := it.Values()
			points = append(points, schema.Point{Ts: ts, Val: val})
		}
	}
	return points
}

func assertPointsEqual(t *testing.T, got, expected []schema.Point) {
	if len(got) != len(expected) {
		t.Fatalf("output mismatch: expected: %v points (%v), got: %v (%v)", len(expected), expected, len(got), got)

	} else {
		for i, g := range got {
			exp := expected[i]
			if exp.Val != g.Val || exp.Ts != g.Ts {
				t.Fatalf("output mismatch at point %d: expected: %v, got: %v", i, exp, g)
			}
		}
	}
}

func TestGetAggregated(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(true)
	mockstore.Reset()
	aggSpan := uint32(5)
	// note: TTL's are ignored
	ret := conf.MustParseRetentions("1s:5s:10s:5,5s:10s:10s:5") // note second raw interval matches aggSpan
	agg := conf.Aggregation{
		Name:              "Default",
		Pattern:           regexp.MustCompile(".*"),
		XFilesFactor:      0.5,
		AggregationMethod: []conf.Method{conf.Sum},
	}

	m := NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), ret, 0, 1, &agg, false, false, 0)
	m.Add(10, 10)
	m.Add(11, 11)
	m.Add(12, 12)
	m.Add(20, 20)
	m.Add(21, 21)
	m.Add(22, 22)
	m.Add(30, 30)
	m.Add(31, 31)
	m.Add(32, 32)

	result, err := m.GetAggregated(consolidation.Sum, aggSpan, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}

	got := append(itersToPoints(result.Iters), result.Points...)

	expected := []schema.Point{
		{Val: 10, Ts: 10},
		{Val: 11 + 12, Ts: 15},
		{Val: 20, Ts: 20},
		{Val: 21 + 22, Ts: 25},
		{Val: 30, Ts: 30},
		{Val: 31 + 32, Ts: 35},
	}
	assertPointsEqual(t, got, expected)
}

func TestGetAggregatedIngestFrom(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(true)
	mockstore.Reset()
	aggSpan := uint32(5)
	ingestFrom := int64(23)
	// note: TTL's are ignored
	ret := conf.MustParseRetentions("1s:5s:10s:5,5s:10s:10s:5") // note second raw interval matches aggSpan
	agg := conf.Aggregation{
		Name:              "Default",
		Pattern:           regexp.MustCompile(".*"),
		XFilesFactor:      0.5,
		AggregationMethod: []conf.Method{conf.Sum},
	}

	m := NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(42), ret, 0, 1, &agg, false, false, ingestFrom)
	m.Add(10, 10)
	m.Add(11, 11)
	m.Add(12, 12)
	m.Add(20, 20)
	m.Add(21, 21)
	m.Add(22, 22)
	m.Add(25, 25)
	m.Add(26, 26)
	m.Add(30, 30)
	m.Add(31, 31)
	m.Add(32, 32)
	m.Add(40, 40)

	result, err := m.GetAggregated(consolidation.Sum, aggSpan, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}

	got := itersToPoints(result.Iters)

	expected := []schema.Point{
		{Val: 26 + 30, Ts: 30},
		{Val: 31 + 32, Ts: 35},
		{Val: 40, Ts: 40},
	}
	assertPointsEqual(t, got, expected)
}

func BenchmarkAggMetricAdd(b *testing.B) {
	mockstore.Reset()
	mockstore.Drop = true
	defer func() {
		mockstore.Drop = false
	}()

	cluster.Init("default", "test", time.Now(), "http", 6060)

	// each chunk contains 180 points
	rets := conf.MustParseRetentions("10s:1000000000s,30min:1")
	metric := NewAggMetric(MockFlusher{mockstore, nil}, test.GetAMKey(0), rets, 0, 10, nil, false, false, 0)

	max := uint32(b.N*10 + 1)
	for t := uint32(1); t < max; t += 10 {
		metric.Add(t, float64(t))
	}
}
