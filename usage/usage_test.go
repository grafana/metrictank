package usage

import (
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
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
func (f *FakeAggMetrics) GetOrCreate(key, name string, schemaId, aggId uint16) mdata.Metric {
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
func (f *FakeAggMetric) Get(from, to uint32) mdata.MetricResult {
	return mdata.MetricResult{Oldest: 0, Iters: make([]chunk.Iter, 0)}
}
func (f *FakeAggMetric) GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) mdata.MetricResult {
	return mdata.MetricResult{Oldest: 0, Iters: make([]chunk.Iter, 0)}
}

func idFor(org int, metric, unit, mtype string, tags []string, interval uint32) string {
	md := schema.MetricData{
		OrgId:    org,
		Metric:   metric,
		Unit:     unit,
		Mtype:    mtype,
		Tags:     tags,
		Interval: int(interval),
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
func assert(interval uint32, epoch int, aggmetrics *FakeAggMetrics, org int, metric, unit, mtype string, ts uint32, val float64, t *testing.T) {
	id := idFor(org, metric, unit, mtype, []string{}, interval)
	m, ok := aggmetrics.Get(id)
	if !ok {
		t.Fatalf("%d seconds in: assert org %d metric %s ts %d val %f -> metric not found", epoch, org, metric, ts, val)
	}
	n := m.(*FakeAggMetric)
	if n.lastVal != val || n.lastTs != ts {
		t.Fatalf("%d seconds in: assert org %d metric %s ts %d val %f -> got ts %d val %f", epoch, org, metric, ts, val, n.lastTs, n.lastVal)
	}
}

func DisabledTestUsageBasic(t *testing.T) {
	mock := clock.NewMock()
	aggmetrics := NewFakeAggMetrics()
	metricIndex := memory.New()
	metricIndex.Init()
	interval := uint32(60)
	u := New(interval, aggmetrics, metricIndex, mock)

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
	assert(interval, 60, aggmetrics, 1, "metrictank.usage.numSeries", "serie", "gauge", 60, 2, t)
	assert(interval, 60, aggmetrics, 1, "metrictank.usage.numPoints", "point", "counter", 60, 2, t)
	assert(interval, 60, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 60, 1, t)
	assert(interval, 60, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 60, 2, t)

	u.Add(1, "foo")
	u.Add(2, "foo")
	mock.Add(60 * time.Second)

	assert(interval, 120, aggmetrics, 1, "metrictank.usage.numSeries", "serie", "gauge", 120, 1, t)
	assert(interval, 120, aggmetrics, 1, "metrictank.usage.numPoints", "point", "counter", 120, 3, t)
	assert(interval, 120, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 120, 1, t)
	assert(interval, 120, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 120, 3, t)

	u.Stop()
}
func DisabledTestUsageMinusOne(t *testing.T) {
	mock := clock.NewMock()
	aggmetrics := NewFakeAggMetrics()
	metricIndex := memory.New()
	metricIndex.Init()
	interval := uint32(60)
	u := New(interval, aggmetrics, metricIndex, mock)

	assertLen(0, aggmetrics, 0, t)

	u.Add(-1, "globally-visible") // but usage only reported to org 1
	u.Add(1, "foo")
	u.Add(2, "bar")
	mock.Add(30 * time.Second)
	assertLen(30, aggmetrics, 0, t)
	mock.Add(29 * time.Second)
	assertLen(59, aggmetrics, 0, t)
	mock.Add(time.Second)
	// not very pretty.. but an easy way to assure that the Usage Reporter
	// goroutine has some time to push the results into our fake aggmetrics
	time.Sleep(20 * time.Millisecond)
	assertLen(60, aggmetrics, 6, t)
	assert(interval, 60, aggmetrics, 1, "metrictank.usage-minus1.numSeries", "serie", "gauge", 60, 1, t)
	assert(interval, 60, aggmetrics, 1, "metrictank.usage-minus1.numPoints", "point", "counter", 60, 1, t)
	assert(interval, 60, aggmetrics, 1, "metrictank.usage.numSeries", "serie", "gauge", 60, 1, t)
	assert(interval, 60, aggmetrics, 1, "metrictank.usage.numPoints", "point", "counter", 60, 1, t)
	assert(interval, 60, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 60, 1, t)
	assert(interval, 60, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 60, 1, t)

	u.Stop()
}
func DisabledTestUsageWrap32(t *testing.T) {
	mock := clock.NewMock()
	aggmetrics := NewFakeAggMetrics()
	metricIndex := memory.New()
	metricIndex.Init()
	interval := uint32(60)
	u := New(interval, aggmetrics, metricIndex, mock)

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
	assert(interval, 60, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 60, 1, t)
	assert(interval, 60, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 60, 4294067295, t)

	for i := 0; i < 1000001; i++ {
		u.Add(2, "foo")
	}
	mock.Add(60 * time.Second)
	assertLen(120, aggmetrics, 2, t)
	assert(interval, 120, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 120, 1, t)
	assert(interval, 120, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 120, 100000, t)

	u.Stop()
}
