package usage

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/raintank/met/helper"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/iter"
	"github.com/raintank/metrictank/mdata"
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

func (f *FakeAggMetrics) GetOrCreate(key string) mdata.Metric {
	f.Lock()
	m, ok := f.Metrics[key]
	if !ok {
		m = &FakeAggMetric{sync.Mutex{}, key, 0, 0}
		f.Metrics[key] = m
	}
	f.Unlock()
	return m
}

type FakeAggMetric struct {
	sync.Mutex
	key     string
	lastTs  uint32
	lastVal float64
}

func (f *FakeAggMetric) Add(ts uint32, val float64) {
	f.Lock()
	f.lastTs = ts
	f.lastVal = val
	f.Unlock()
}

// we won't use this
func (f *FakeAggMetric) Get(from, to uint32) (uint32, []iter.Iter) {
	return 0, make([]iter.Iter, 0)
}
func (f *FakeAggMetric) GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (uint32, []iter.Iter) {
	return 0, make([]iter.Iter, 0)
}

func idFor(org int, metric, unit, mtype string, tags []string) string {
	md := schema.MetricData{
		OrgId:    org,
		Metric:   metric,
		Unit:     unit,
		Mtype:    mtype,
		Tags:     tags,
		Interval: 1,
	}
	md.SetId()
	return md.Id
}

// wait executes fn which can do various assertions and panics when things aren't right
// it will recover the panic, and keep retrying up to every millisecond up to max milliseconds.
// if the error keeps happening until after the deadline, it reports it as a test failure.
func wait(max int, aggmetrics *FakeAggMetrics, t *testing.T, fn func(aggmetrics *FakeAggMetrics)) {
	execute := func() (err error) {
		defer func(errp *error) {
			e := recover()
			if e != nil {
				if _, ok := e.(runtime.Error); ok {
					panic(e)
				}
				if err, ok := e.(error); ok {
					*errp = err
				} else if errStr, ok := e.(string); ok {
					*errp = errors.New(errStr)
				} else {
					*errp = fmt.Errorf("%v", e)
				}
			}
			return
		}(&err)
		fn(aggmetrics)
		return err
	}
	var err error
	for i := 1; i <= max; i++ {
		err = execute()
		if err == nil {
			break
		}
		//fmt.Printf("sleeping %d/%d\n", i, max)
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatalf("waited %d ms, then: %s", max, err)
	}
}

// set t to nil to trigger a panic, useful for inside wait()
func assertLen(epoch int, aggmetrics *FakeAggMetrics, l int, t *testing.T) {
	aggmetrics.Lock()
	defer aggmetrics.Unlock()
	if len(aggmetrics.Metrics) != l {
		e := fmt.Sprintf("%d ms in: there should be %d metrics at this point, not %d", epoch, l, len(aggmetrics.Metrics))
		if t != nil {
			t.Fatal(e)
		} else {
			panic(e)
		}
	}
}

// set t to nil to trigger a panic, useful for inside wait()
func assert(step int, aggmetrics *FakeAggMetrics, org int, metric, unit, mtype string, val float64, t *testing.T) {
	id := idFor(org, metric, unit, mtype, []string{})
	m, ok := aggmetrics.Get(id)
	if !ok {
		e := fmt.Sprintf("step %d: assert org %d metric %s val %f -> metric not found", step, org, metric, val)
		if t != nil {
			t.Fatal(e)
		} else {
			panic(e)
		}
	}
	n := m.(*FakeAggMetric)
	n.Lock()
	defer n.Unlock()
	if n.lastVal != val {
		e := fmt.Sprintf("step %d: assert org %d metric %s val %f -> got val %f", step, org, metric, val, n.lastVal)
		if t != nil {
			t.Fatal(e)
		} else {
			panic(e)
		}
	}
}

func TestUsageBasic(t *testing.T) {
	aggmetrics := NewFakeAggMetrics()
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	mdata.InitMetrics(stats)
	metricIndex := memory.New()
	metricIndex.Init(stats)
	interval := 60
	u := New(time.Duration(interval)*time.Millisecond, 1, aggmetrics, metricIndex)

	assertLen(0, aggmetrics, 0, t)

	u.Add(1, "foo")
	u.Add(1, "bar")
	u.Add(2, "foo")
	u.Add(2, "foo")
	wait(60, aggmetrics, t, func(aggmetrics *FakeAggMetrics) {
		assertLen(1, aggmetrics, 4, nil)
		assert(1, aggmetrics, 1, "metrictank.usage.numSeries", "serie", "gauge", 2, nil)
		assert(1, aggmetrics, 1, "metrictank.usage.numPoints", "point", "counter", 2, nil)
		assert(1, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 1, nil)
		assert(1, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 2, nil)
	})

	u.Add(1, "foo")
	u.Add(2, "foo")
	u.Add(3, "foo")
	wait(60, aggmetrics, t, func(aggmetrics *FakeAggMetrics) {
		assertLen(2, aggmetrics, 6, nil)
		assert(2, aggmetrics, 1, "metrictank.usage.numSeries", "serie", "gauge", 1, nil)
		assert(2, aggmetrics, 1, "metrictank.usage.numPoints", "point", "counter", 3, nil)
		assert(2, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 1, nil)
		assert(2, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 3, nil)
		assert(2, aggmetrics, 3, "metrictank.usage.numSeries", "serie", "gauge", 1, nil)
		assert(2, aggmetrics, 3, "metrictank.usage.numPoints", "point", "counter", 1, nil)
	})

	u.Stop()
}

func testUsageMinusOne(t *testing.T) {
	aggmetrics := NewFakeAggMetrics()
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	mdata.InitMetrics(stats)
	metricIndex := memory.New()
	metricIndex.Init(stats)
	interval := 60
	u := New(time.Duration(interval)*time.Millisecond, 1, aggmetrics, metricIndex)

	assertLen(0, aggmetrics, 0, t)

	u.Add(-1, "globally-visible") // but usage only reported to org 1
	u.Add(1, "foo")
	u.Add(2, "bar")
	wait(60, aggmetrics, t, func(aggmetrics *FakeAggMetrics) {
		assertLen(1, aggmetrics, 6, nil)
		assert(1, aggmetrics, 1, "metrictank.usage-minus1.numSeries", "serie", "gauge", 1, nil)
		assert(1, aggmetrics, 1, "metrictank.usage-minus1.numPoints", "point", "counter", 1, nil)
		assert(1, aggmetrics, 1, "metrictank.usage.numSeries", "serie", "gauge", 1, nil)
		assert(1, aggmetrics, 1, "metrictank.usage.numPoints", "point", "counter", 1, nil)
		assert(1, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 1, nil)
		assert(1, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 1, nil)
	})

	u.Stop()
}

func TestUsageWrap32(t *testing.T) {
	aggmetrics := NewFakeAggMetrics()
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	mdata.InitMetrics(stats)
	metricIndex := memory.New()
	metricIndex.Init(stats)
	interval := 60
	u := New(time.Duration(interval)*time.Millisecond, 1, aggmetrics, metricIndex)

	// max uint32 is 4294967295, let's verify the proper wrapping around that
	// pretend an insert maxuint32 -900000

	assertLen(0, aggmetrics, 0, t)
	u.Add(2, "foo")
	u.set(2, "foo", 4294067295)
	wait(60, aggmetrics, t, func(aggmetrics *FakeAggMetrics) {
		assertLen(1, aggmetrics, 2, nil)
		assert(1, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 1, t)
		assert(1, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 4294067295, t)
	})

	for i := 0; i < 1000001; i++ {
		u.Add(2, "foo")
	}
	wait(60, aggmetrics, t, func(aggmetrics *FakeAggMetrics) {
		assertLen(2, aggmetrics, 2, nil)
		assert(2, aggmetrics, 2, "metrictank.usage.numSeries", "serie", "gauge", 1, nil)
		assert(2, aggmetrics, 2, "metrictank.usage.numPoints", "point", "counter", 100000, nil)
	})

	u.Stop()
}
