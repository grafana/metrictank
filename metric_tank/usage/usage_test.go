package usage

import (
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/raintank/met/helper"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metric_tank/iter"
	"github.com/raintank/raintank-metric/metric_tank/mdata"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/schema"
)

type FakeAggMetrics struct {
	sync.Mutex
	Metrics map[string]*FakeAggMetric
}

func NewFakeAggMetrics() *FakeAggMetrics {
	return &FakeAggMetrics{
		Metrics: make(map[string]*FakeAggMetric),
	}
}

func (f *FakeAggMetrics) Get(key string) (mdata.Metric, bool) {
	f.Lock()
	m, ok := f.Metrics[key]
	f.Unlock()
	return m, ok
}
func (f *FakeAggMetrics) GetOrCreate(key string) mdata.Metric {
	f.Lock()
	m, ok := f.Metrics[key]
	if !ok {
		m = &FakeAggMetric{key, 0, 0}
		f.Metrics[key] = m
	}
	f.Unlock()
	return m
}

type FakeAggMetric struct {
	key     string
	lastTs  uint32
	lastVal float64
}

func (f *FakeAggMetric) Add(ts uint32, val float64) {
	f.lastTs = ts
	f.lastVal = val
}

// we won't use this
func (f *FakeAggMetric) Get(from, to uint32) (uint32, []iter.Iter) {
	return 0, make([]iter.Iter, 0)
}
func (f *FakeAggMetric) GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (uint32, []iter.Iter) {
	return 0, make([]iter.Iter, 0)
}

func idFor(org int, name string, tags []string) string {
	md := schema.MetricData{
		OrgId: org,
		Name:  name,
		Tags:  tags,
	}
	md.SetId()
	return md.Id
}

func assertLen(epoch int, aggmetrics *FakeAggMetrics, l int, t *testing.T) {
	aggmetrics.Lock()
	if len(aggmetrics.Metrics) != l {
		t.Fatalf("%d seconds in: there should be %d metrics at this point, not %d", epoch, l, len(aggmetrics.Metrics))
	}
	aggmetrics.Unlock()
}
func assert(epoch int, aggmetrics *FakeAggMetrics, org int, name string, ts uint32, val float64, t *testing.T) {
	id := idFor(org, name, []string{})
	m, ok := aggmetrics.Get(id)
	if !ok {
		t.Fatalf("%d seconds in: assert org %d name %s ts %d val %f -> metric not found", epoch, org, name, ts, val)
	}
	n := m.(*FakeAggMetric)
	if n.lastVal != val || n.lastTs != ts {
		t.Fatalf("%d seconds in: assert org %d name %s ts %d val %f -> got ts %d val %f", epoch, org, name, ts, val, n.lastTs, n.lastVal)
	}
}

func TestUsageBasic(t *testing.T) {
	mock := clock.NewMock()
	aggmetrics := NewFakeAggMetrics()
	stats, _ := helper.New(false, "", "standard", "metrics_tank", "")
	mdata.InitMetrics(stats)
	defCache := defcache.New(metricdef.NewDefsMockConcurrent(), stats)
	u := New(60, aggmetrics, defCache, mock)

	assertLen(0, aggmetrics, 0, t)

	u.Add(1, "foo")
	u.Add(1, "bar")
	u.Add(2, "foo")
	mock.Add(30 * time.Second)
	assertLen(30, aggmetrics, 0, t)
	mock.Add(29 * time.Second)
	assertLen(59, aggmetrics, 0, t)

	u.Add(2, "foo")
	mock.Add(time.Second)
	assertLen(60, aggmetrics, 4, t)
	assert(60, aggmetrics, 1, "metric_tank.usage.numSeries", 60, 2, t)
	assert(60, aggmetrics, 1, "metric_tank.usage.numPoints", 60, 2, t)
	assert(60, aggmetrics, 2, "metric_tank.usage.numSeries", 60, 1, t)
	assert(60, aggmetrics, 2, "metric_tank.usage.numPoints", 60, 2, t)

	u.Add(1, "foo")
	u.Add(2, "foo")
	mock.Add(60 * time.Second)

	assert(120, aggmetrics, 1, "metric_tank.usage.numSeries", 120, 1, t)
	assert(120, aggmetrics, 1, "metric_tank.usage.numPoints", 120, 3, t)
	assert(120, aggmetrics, 2, "metric_tank.usage.numSeries", 120, 1, t)
	assert(120, aggmetrics, 2, "metric_tank.usage.numPoints", 120, 3, t)

	u.Stop()
}
func TestUsageMinusOne(t *testing.T) {
	mock := clock.NewMock()
	aggmetrics := NewFakeAggMetrics()
	stats, _ := helper.New(false, "", "standard", "metrics_tank", "")
	mdata.InitMetrics(stats)
	defCache := defcache.New(metricdef.NewDefsMockConcurrent(), stats)
	u := New(60, aggmetrics, defCache, mock)

	assertLen(0, aggmetrics, 0, t)

	u.Add(-1, "globally-visible") // but usage only reported to org 1
	u.Add(1, "foo")
	u.Add(2, "bar")
	mock.Add(30 * time.Second)
	assertLen(30, aggmetrics, 0, t)
	mock.Add(29 * time.Second)
	assertLen(59, aggmetrics, 0, t)
	mock.Add(time.Second)
	assertLen(60, aggmetrics, 6, t)
	assert(60, aggmetrics, 1, "metric_tank.usage-minus1.numSeries", 60, 1, t)
	assert(60, aggmetrics, 1, "metric_tank.usage-minus1.numPoints", 60, 1, t)
	assert(60, aggmetrics, 1, "metric_tank.usage.numSeries", 60, 1, t)
	assert(60, aggmetrics, 1, "metric_tank.usage.numPoints", 60, 1, t)
	assert(60, aggmetrics, 2, "metric_tank.usage.numSeries", 60, 1, t)
	assert(60, aggmetrics, 2, "metric_tank.usage.numPoints", 60, 1, t)

	u.Stop()
}
func TestUsageWrap32(t *testing.T) {
	mock := clock.NewMock()
	aggmetrics := NewFakeAggMetrics()
	stats, _ := helper.New(false, "", "standard", "metrics_tank", "")
	mdata.InitMetrics(stats)
	defCache := defcache.New(metricdef.NewDefsMockConcurrent(), stats)
	u := New(60, aggmetrics, defCache, mock)

	// max uint32 is 4294967295, let's verify the proper wrapping around that
	// pretend an insert maxuint32 -900000

	assertLen(0, aggmetrics, 0, t)
	u.Add(2, "foo")
	u.set(2, "foo", 4294067295)
	mock.Add(30 * time.Second)
	assertLen(30, aggmetrics, 0, t)
	mock.Add(29 * time.Second)
	assertLen(59, aggmetrics, 0, t)
	mock.Add(time.Second)
	assertLen(60, aggmetrics, 2, t)
	assert(60, aggmetrics, 2, "metric_tank.usage.numSeries", 60, 1, t)
	assert(60, aggmetrics, 2, "metric_tank.usage.numPoints", 60, 4294067295, t)

	for i := 0; i < 1000001; i++ {
		u.Add(2, "foo")
	}
	mock.Add(60 * time.Second)
	assertLen(120, aggmetrics, 2, t)
	assert(120, aggmetrics, 2, "metric_tank.usage.numSeries", 120, 1, t)
	assert(120, aggmetrics, 2, "metric_tank.usage.numPoints", 120, 100000, t)

	u.Stop()
}
