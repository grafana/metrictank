package mdata

import (
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/mdata/cache"
	"github.com/grafana/metrictank/test"
)

var mockstore = NewMockStore()

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

	numChunks, chunkAddCount, chunkSpan := uint32(5), uint32(10), uint32(300)
	ret := []conf.Retention{conf.NewRetentionMT(1, 1, chunkSpan, numChunks, true)}
	agg := NewAggMetric(mockstore, &mockCache, test.GetAMKey(42), ret, 0, nil, false)

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

	ret := []conf.Retention{conf.NewRetentionMT(1, 1, 120, 5, true)}
	c := NewChecker(t, NewAggMetric(mockstore, &cache.MockCache{}, test.GetAMKey(42), ret, 0, nil, false))

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
	ret := []conf.Retention{conf.NewRetentionMT(1, 1, 120, 5, true)}
	c := NewChecker(t, NewAggMetric(mockstore, &cache.MockCache{}, test.GetAMKey(42), ret, 10, &agg, false))

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

	metricsTooOld.SetUint32(0)

	// adds 10 entries that are out of order and the reorder buffer should order the first 9
	// the last item (365) will be too old, so it increases metricsTooOld counter
	for i := uint32(374); i > 364; i-- {
		c.Add(i, float64(i))
	}
	c.DropPointByTs(365)

	// get subranges
	c.Verify(true, 120, 380, 121, 375)

	// one point has been added out of order and too old for the buffer to reorder
	if metricsTooOld.Peek() != 1 {
		t.Fatalf("Expected the out of order count to be 1, not %d", metricsTooOld.Peek())
	}
}

func TestAggMetricDropFirstChunk(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(true)
	mockstore.Reset()
	chunkSpan := uint32(10)
	numChunks := uint32(5)
	ret := []conf.Retention{conf.NewRetentionMT(1, 1, chunkSpan, numChunks, true)}
	m := NewAggMetric(mockstore, &cache.MockCache{}, test.GetAMKey(42), ret, 0, nil, true)
	m.Add(10, 10)
	m.Add(11, 11)
	m.Add(12, 12)
	m.Add(20, 20)
	m.Add(21, 21)
	m.Add(22, 22)
	m.Add(30, 30)
	m.Add(31, 31)
	m.Add(32, 32)
	m.Add(40, 40)
	itgens, err := mockstore.Search(test.NewContext(), test.GetAMKey(42), 0, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(itgens) != 2 || itgens[0].T0 != 20 || itgens[1].T0 != 30 {
		t.Fatalf("expected 2 itgens for chunks 20 and 30 since the first one should be dropped. Got %v", itgens)
	}
}

func BenchmarkAggMetricAdd(b *testing.B) {
	mockstore.Reset()
	mockstore.Drop = true
	defer func() {
		mockstore.Drop = false
	}()

	cluster.Init("default", "test", time.Now(), "http", 6060)

	retentions := conf.Retentions{
		{
			SecondsPerPoint: 10,
			NumberOfPoints:  10e9, // TTL
			ChunkSpan:       1800, // 30 min. contains 180 points at 10s resolution
			NumChunks:       1,
			Ready:           true,
		},
	}

	metric := NewAggMetric(mockstore, &cache.MockCache{}, test.GetAMKey(0), retentions, 0, nil, false)

	max := uint32(b.N*10 + 1)
	for t := uint32(1); t < max; t += 10 {
		metric.Add(t, float64(t))
	}
}
