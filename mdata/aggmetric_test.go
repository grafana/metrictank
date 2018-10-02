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

	ret := []conf.Retention{conf.NewRetentionMT(1, 1, 100, 5, true)}
	c := NewChecker(t, NewAggMetric(mockstore, &cache.MockCache{}, test.GetAMKey(42), ret, 0, nil, false))

	// basic case, single range
	c.Add(101, 101)
	c.Verify(true, 100, 200, 101, 101)
	c.Add(105, 105)
	c.Verify(true, 100, 199, 101, 105)
	c.Add(115, 115)
	c.Add(125, 125)
	c.Add(135, 135)
	c.Verify(true, 100, 199, 101, 135)

	// add new ranges, aligned and unaligned
	c.Add(200, 200)
	c.Add(315, 315)
	c.Verify(true, 100, 399, 101, 315)
	c.Verify(false, 100, 399, 101, 315)

	// get subranges
	c.Verify(true, 120, 299, 101, 200)
	c.Verify(true, 220, 299, 200, 200)
	c.Verify(true, 312, 330, 315, 315)

	// border dancing. good for testing inclusivity and exclusivity
	c.Verify(true, 100, 199, 101, 135)
	c.Verify(true, 100, 200, 101, 135)
	c.Verify(true, 100, 201, 101, 200)
	c.Verify(true, 198, 199, 101, 135)
	c.Verify(true, 199, 200, 101, 135)
	c.Verify(true, 200, 201, 200, 200)
	c.Verify(true, 201, 202, 200, 200)
	c.Verify(true, 299, 300, 200, 200)
	c.Verify(true, 300, 301, 315, 315)

	// skipping
	c.Add(510, 510)
	c.Add(512, 512)
	c.Verify(true, 100, 599, 101, 512)

	// basic wraparound
	c.Add(610, 610)
	c.Add(612, 612)
	c.Add(710, 710)
	c.Add(712, 712)
	// TODO would be nice to test that it panics when requesting old range. something with recover?
	//c.Verify(true, 100, 799, 101, 512)

	// largest range we have so far
	c.Verify(true, 300, 799, 315, 712)
	// a smaller range
	c.Verify(true, 502, 799, 510, 712)

	// the circular buffer had these ranges:
	// 100 200 300 skipped 500
	// then we made it:
	// 600 700 300 skipped 500
	// now we want to do another wrap around with skip (must have cleared old data)
	// let's jump to 1200. the accessible range should then be 800-1200
	// clea 1200 clea clea clea
	// we can't (and shouldn't, due to abstraction) test the clearing itself
	// but we just check we only get this point
	c.Add(1299, 1299)
	// TODO: implement skips and enable this
	//	c.Verify(true, 800, 1299, 1299, 1299)
}

func TestAggMetricWithReorderBuffer(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	agg := conf.Aggregation{
		Name:              "Default",
		Pattern:           regexp.MustCompile(".*"),
		XFilesFactor:      0.5,
		AggregationMethod: []conf.Method{conf.Avg},
	}
	ret := []conf.Retention{conf.NewRetentionMT(1, 1, 100, 5, true)}
	c := NewChecker(t, NewAggMetric(mockstore, &cache.MockCache{}, test.GetAMKey(42), ret, 10, &agg, false))

	// basic adds and verifies with test data
	c.Add(101, 101)
	c.Verify(true, 100, 200, 101, 101)
	c.Add(105, 105)
	c.Verify(true, 100, 199, 101, 105)
	c.Add(115, 115)
	c.Add(125, 125)
	c.Add(135, 135)
	c.Verify(true, 100, 199, 101, 135)
	c.Add(200, 200)
	c.Add(315, 315)
	c.Verify(true, 100, 399, 101, 315)

	metricsTooOld.SetUint32(0)

	// adds 10 entries that are out of order and the reorder buffer should order the first 9
	// the last item (305) will be too old, so it increases metricsTooOld counter
	for i := uint32(314); i > 304; i-- {
		c.Add(i, float64(i))
	}
	c.DropPointByTs(305)

	// get subranges
	c.Verify(true, 100, 320, 101, 315)

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
	if len(itgens) != 2 || itgens[0].Ts != 20 || itgens[1].Ts != 30 {
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
