package mdata

import (
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/conf"
	"github.com/raintank/metrictank/mdata/cache"
)

var dnstore = NewDevnullStore()

type point struct {
	ts  uint32
	val float64
}

type points []point

func (a points) Len() int           { return len(a) }
func (a points) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a points) Less(i, j int) bool { return a[i].ts < a[j].ts }

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

// always add points in ascending order, never same ts!
func (c *Checker) Add(ts uint32, val float64) {
	c.agg.Add(ts, val)
	c.points = append(c.points, point{ts, val})
}

func (c *Checker) DropPointByTs(ts uint32) {
	i := 0
	for {
		if i == len(c.points) {
			return
		}
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
	sort.Sort(points(c.points))
	cluster.Manager.SetPrimary(primary)
	res := c.agg.Get(from, to)
	// we don't do checking or fancy logic, it is assumed that the caller made sure first and last are ts of actual points
	var pi int // index of first point we want
	var pj int // index of last point we want
	for pi = 0; c.points[pi].ts != first; pi++ {
	}
	for pj = pi; c.points[pj].ts != last; pj++ {
	}
	c.t.Logf("verifying AggMetric.Get(%d,%d) =?= %d <= ts <= %d", from, to, first, last)
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
	for _, point := range res.Raw {
		index++
		if index > pj {
			c.t.Fatalf("Raw: Values()=(%v,%v), want end of stream\n", point.Ts, point.Val)
		}
		if c.points[index].ts != point.Ts || c.points[index].val != point.Val {
			c.t.Fatalf("Raw: Values()=(%v,%v), want (%v,%v)\n", point.Ts, point.Val, c.points[index].ts, c.points[index].val)
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
	dnstore.Reset()
	defer dnstore.Reset()

	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(primary)

	callCount := uint32(0)
	calledCb := make(chan bool)

	mockCache := cache.MockCache{}
	mockCache.CacheIfHotCb = func() { calledCb <- true }

	numChunks, chunkAddCount, chunkSpan := uint32(5), uint32(10), uint32(300)
	ret := []conf.Retention{conf.NewRetentionMT(1, 1, chunkSpan, numChunks, true)}
	agg := NewAggMetric(dnstore, &mockCache, "foo", ret, nil, false)

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
		if dnstore.AddCount != chunkAddCount-1 {
			t.Fatalf("there should have been %d chunk adds on store, but go %d", chunkAddCount-1, dnstore.AddCount)
		}
	} else {
		if dnstore.AddCount != 0 {
			t.Fatalf("there should have been %d chunk adds on store, but go %d", 0, dnstore.AddCount)
		}
	}
}

func TestAggMetric(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	ret := []conf.Retention{conf.NewRetentionMT(1, 1, 100, 5, true)}
	c := NewChecker(t, NewAggMetric(dnstore, &cache.MockCache{}, "foo", ret, nil, false))

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

	// verify as secondary node. Data from the first chunk should not be returned.
	c.Verify(false, 100, 399, 200, 315)

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

func TestAggMetricWithWriteBuffer(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	agg := conf.Aggregation{
		Name:              "Default",
		Pattern:           regexp.MustCompile(".*"),
		XFilesFactor:      0.5,
		AggregationMethod: []conf.Method{conf.Avg},
		WriteBufferConf: &conf.WriteBufferConf{
			ReorderWindow: 10,
			FlushMin:      10,
		},
	}
	ret := []conf.Retention{conf.NewRetentionMT(1, 1, 100, 5, true)}
	c := NewChecker(t, NewAggMetric(dnstore, &cache.MockCache{}, "foo", ret, &agg, false))

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

	metricsTooOld.SetUint32(0)

	// adds 14 entries that are out of order and the write buffer should order the first 13
	// including the previous 7 it will then reach 20 which is = reorder window + flush min, so it causes a flush
	// the last item (14th) will be added out of order, after the buffer is flushed, so it increases metricsTooOld
	for i := uint32(314); i > 300; i-- {
		c.Add(i, float64(i))
	}
	c.DropPointByTs(301)

	// get subranges
	c.Verify(true, 100, 320, 101, 315)

	// one point has been added out of order and too old for the buffer to reorder
	if metricsTooOld.Peek() != 1 {
		t.Fatalf("Expected the out off order count to be 1")
	}
}

func TestAggMetricDropFirstChunk(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(true)
	store := NewMockStore()
	chunkSpan := uint32(10)
	numChunks := uint32(5)
	ret := []conf.Retention{conf.NewRetentionMT(1, 1, chunkSpan, numChunks, true)}
	m := NewAggMetric(store, &cache.MockCache{}, "foo", ret, nil, true)
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
	itgens, err := store.Search("foo", 0, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(itgens) != 2 || itgens[0].Ts != 20 || itgens[1].Ts != 30 {
		t.Fatalf("expected 2 itgens for chunks 20 and 30 since the first one should be dropped. Got %v", itgens)
	}
}

// basic expected RAM usage for 1 iteration (= 1 days)
// 1000 metrics * (3600 * 24 / 10 ) points per metric * 1.3 B/point = 11 MB
// 1000 metrics * 5 agg metrics per metric * (3600 * 24 / 300) points per aggmetric * 1.3B/point = 1.9 MB
// total -> 13 MB
// go test -run=XX -bench=Bench -benchmem -v -memprofile mem.out
// go tool pprof -inuse_space metrictank.test mem.out -> shows 25 MB in use

// TODO update once we clean old data, then we should look at numChunks
func BenchmarkAggMetrics1000Metrics1Day(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	// we will store 10s metrics in 5 chunks of 2 hours
	// aggregate them in 5min buckets, stored in 1 chunk of 24hours
	SetSingleAgg(conf.Avg, conf.Min, conf.Max)
	SetSingleSchema(
		conf.NewRetentionMT(1, 84600, 2*3600, 5, true),
		conf.NewRetentionMT(300, 30*84600, 24*3600, 1, true),
	)
	chunkMaxStale := uint32(3600)
	metricMaxStale := uint32(21600)

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("hello.this.is.a.test.key.%d", i)
	}

	metrics := NewAggMetrics(dnstore, &cache.MockCache{}, false, chunkMaxStale, metricMaxStale, 0)

	maxT := 3600 * 24 * uint32(b.N) // b.N in days
	for t := uint32(1); t < maxT; t += 10 {
		for metricI := 0; metricI < 1000; metricI++ {
			k := keys[metricI]
			m := metrics.GetOrCreate(k, k, 0, 0)
			m.Add(t, float64(t))
		}
	}
}

func BenchmarkAggMetrics1kSeries2Chunks1kQueueSize(b *testing.B) {
	chunkMaxStale := uint32(3600)
	metricMaxStale := uint32(21600)

	SetSingleAgg(conf.Avg, conf.Min, conf.Max)
	SetSingleSchema(
		conf.NewRetentionMT(1, 84600, 600, 5, true),
		conf.NewRetentionMT(300, 84600, 24*3600, 2, true),
	)

	cluster.Init("default", "test", time.Now(), "http", 6060)

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("hello.this.is.a.test.key.%d", i)
	}

	metrics := NewAggMetrics(dnstore, &cache.MockCache{}, false, chunkMaxStale, metricMaxStale, 0)

	maxT := uint32(1200)
	for t := uint32(1); t < maxT; t += 10 {
		for metricI := 0; metricI < 1000; metricI++ {
			k := keys[metricI]
			m := metrics.GetOrCreate(k, k, 0, 0)
			m.Add(t, float64(t))
		}
	}
}

func BenchmarkAggMetrics10kSeries2Chunks10kQueueSize(b *testing.B) {
	chunkMaxStale := uint32(3600)
	metricMaxStale := uint32(21600)

	SetSingleAgg(conf.Avg, conf.Min, conf.Max)
	SetSingleSchema(
		conf.NewRetentionMT(1, 84600, 600, 5, true),
		conf.NewRetentionMT(300, 84600, 24*3600, 2, true),
	)

	cluster.Init("default", "test", time.Now(), "http", 6060)

	keys := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = fmt.Sprintf("hello.this.is.a.test.key.%d", i)
	}

	metrics := NewAggMetrics(dnstore, &cache.MockCache{}, false, chunkMaxStale, metricMaxStale, 0)

	maxT := uint32(1200)
	for t := uint32(1); t < maxT; t += 10 {
		for metricI := 0; metricI < 10000; metricI++ {
			k := keys[metricI]
			m := metrics.GetOrCreate(k, k, 0, 0)
			m.Add(t, float64(t))
		}
	}
}

func BenchmarkAggMetrics100kSeries2Chunks100kQueueSize(b *testing.B) {
	chunkMaxStale := uint32(3600)
	metricMaxStale := uint32(21600)

	SetSingleAgg(conf.Avg, conf.Min, conf.Max)
	SetSingleSchema(
		conf.NewRetentionMT(1, 84600, 600, 5, true),
		conf.NewRetentionMT(300, 84600, 24*3600, 2, true),
	)

	cluster.Init("default", "test", time.Now(), "http", 6060)

	keys := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		keys[i] = fmt.Sprintf("hello.this.is.a.test.key.%d", i)
	}

	metrics := NewAggMetrics(dnstore, &cache.MockCache{}, false, chunkMaxStale, metricMaxStale, 0)

	maxT := uint32(1200)
	for t := uint32(1); t < maxT; t += 10 {
		for metricI := 0; metricI < 100000; metricI++ {
			k := keys[metricI]
			m := metrics.GetOrCreate(k, k, 0, 0)
			m.Add(t, float64(t))
		}
	}
}
