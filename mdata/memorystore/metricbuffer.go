package memorystore

import (
	"sort"
	"sync"

	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type Range struct {
	low  uint32
	high uint32
}

// Datapoints received with an interval of more then 1day are rounded to the nearest number of days.
// Datapoints received with an interval > 1hour are rounded to the nearest number of hours.
// Otherwise, if the interval is within +/- 10% of a ValidInterval we use that.
var ValidIntervals = []uint32{1, 5, 10, 15, 20, 30, 60, 120, 300, 600, 1800, 3600}
var IntervalRanges map[uint32]Range

func init() {
	IntervalRanges = make(map[uint32]Range)
	for _, i := range ValidIntervals {
		low := uint32(float64(i) * 0.9)
		if i == 1 {
			low = 1
		}
		IntervalRanges[i] = Range{
			low:  low,
			high: uint32((float64(i) * 1.1) + 0.5),
		}
	}
}

type SortedPoints []schema.Point

func (a SortedPoints) Len() int           { return len(a) }
func (a SortedPoints) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedPoints) Less(i, j int) bool { return a[i].Ts < a[j].Ts }

type MetricBuffer struct {
	sync.RWMutex
	key  string
	buf  []schema.Point
	data *schema.MetricData
}

func NewMetricBuffer(key string, data *schema.MetricData) *MetricBuffer {
	return &MetricBuffer{
		key:  key,
		data: data,
		buf:  make([]schema.Point, 0, 3),
	}
}

func (m *MetricBuffer) Add(p schema.DataPoint, partition int32) {
	m.Lock()
	defer m.Unlock()
	if data := p.Data(); data != nil {
		m.data = data
	}
	// dont add duplicate points.
	if len(m.buf) > 0 && p.GetTime() == m.buf[len(m.buf)-1].Ts {
		return
	}

	m.buf = append(m.buf, schema.Point{
		Ts:  p.GetTime(),
		Val: p.GetValue(),
	})
	log.Info("metricBuffer for %s has %d points", m.key, len(m.buf))

	if m.data != nil {
		var sorted bool
		if m.data.Interval <= 0 && len(m.buf) >= 3 {
			// we dont yet know the interval, but we have enough points to work it out.
			sort.Sort(SortedPoints(m.buf))
			interval := getInterval(m.buf)

			if interval == 0 {
				// we havent been able to determine the interval yet.
				return
			}
			sorted = true
			m.data.Interval = interval
			m.data.SetId()
			log.Info("detected Interval for %s. it is %d", m.key, interval)
		}

		if m.data.Interval > 0 {
			if !sorted {
				sort.Sort(SortedPoints(m.buf))
			}
			// we now have enough info to move to a regular AggMetric
			m.data.Time = int64(m.buf[len(m.buf)-1].Ts)
			m.data.Value = m.buf[len(m.buf)-1].Val

			// add to the index
			archive := mdata.Idx.AddOrUpdate(m.data, partition)
			agg := mdata.Aggregations.Get(archive.AggId)
			schema := mdata.Schemas.Get(archive.SchemaId)
			reorderWindow := schema.ReorderWindow

			if m.data.Id != m.key && reorderWindow < 3 {
				reorderWindow = 3
			}
			var aggMetric mdata.Metric
			aggMetric = NewAggMetric(m.data.Id, schema.Retentions, reorderWindow, &agg)
			// if our full MetricData has different Id to the datapoints we received (this is true if the auto detected the interval)
			// then add a second item to the MemoryStore index with both pointing to the same AggMetric.
			if m.data.Id != m.key {
				log.Info("metric with key %s is now known by key %s", m.key, m.data.Id)
				aggMetric, _ = mdata.MemoryStore.LoadOrStore(m.data.Id, aggMetric)
				aggMetric.(*AggMetric).AlternateKey = m.key
			}
			log.Info("metric with key %s is being converted to an AggMetric under key %s", m.key, m.data.Id)
			mdata.MemoryStore.Store(m.key, aggMetric)

			// add all of the points in the buffer.
			for i := 0; i < len(m.buf); i++ {
				aggMetric.(*AggMetric).AddPoint(m.buf[i].Ts, m.buf[i].Val)
			}

			// remove data, just incase another datapoint is already trying to add points to this MetricBuffer
			// It will be blocked by the Lock until we are done.
			m.buf = nil
			return
		}
	}
}

func (m *MetricBuffer) Get(from, to uint32) mdata.Result {
	return mdata.Result{}
}

func (m *MetricBuffer) GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (mdata.Result, error) {
	return mdata.Result{}, nil
}

func (m *MetricBuffer) SyncChunkSaveState(ts uint32, consolidator consolidation.Consolidator, aggSpan uint32) {
	return
}

func (m *MetricBuffer) GC(chunkMinTs, metricMinTs uint32) bool {
	return false
}

func getInterval(points []schema.Point) int {
	// points are sorted by Ts

	deltas := make(map[uint32]int)
	interval := uint32(0)
	// walk through the points, moving from newest to oldest and get the delta
	// between it and the previous point.  If we see two deltas that are the same,
	// we use that as the interval of the metric.  Otherwise we we will need to wait
	// until we have more points
	for i := len(points) - 1; i > 0; i-- {
		delta := points[i].Ts - points[i-1].Ts
		switch {
		case delta >= 86400:
			// round to the nearest day
			delta = 86400 * uint32((float64(delta)/86400.0)+0.5)
		case delta >= 3600:
			// round to the nearest hour
			delta = 3600 * uint32((float64(delta)/3600.0)+0.5)
		default:
			if _, ok := IntervalRanges[delta]; !ok {
				for i, r := range IntervalRanges {
					if delta >= r.low && delta <= r.high {
						delta = i
						break
					}
				}
			}
		}
		if _, ok := deltas[delta]; ok {
			// BINGO. the same delta has been seen twice.
			interval = delta
			break
		} else {
			deltas[delta]++
		}
	}
	if interval == 0 {
		log.Info("failed to find interval.  deltas=%+v", deltas)
	}

	return int(interval)
}
