package mdata

import (
	"github.com/grafana/metrictank/mdata/errors"
	"github.com/grafana/metrictank/schema"
)

// ReorderBuffer keeps a window of data during which it is ok to send data out of order.
// The reorder buffer itself is not thread safe because it is only used by AggMetric,
// which is thread safe, so there is no locking in the buffer.
//
// newest=0 may mean no points added yet, or newest point is at position 0.
// we use the Ts of points in the buffer to check for valid points. Ts == 0 means no point
// in particular newest.Ts == 0 means the buffer is empty
// the buffer is evenly spaced (points are `interval` apart) and may be sparsely populated
type ReorderBuffer struct {
	newest      uint32         // index of newest buffer entry
	interval    uint32         // metric interval
	buf         []schema.Point // the actual buffer holding the data
	allowUpdate bool           // whether or not to allow overwriting data points with same timestamps
}

func NewReorderBuffer(reorderWindow, interval uint32, allowUpdate bool) *ReorderBuffer {
	return &ReorderBuffer{
		interval:    interval,
		buf:         make([]schema.Point, reorderWindow),
		allowUpdate: allowUpdate,
	}
}

// Add adds the point if it falls within the window.
// it returns points that have been purged out of the buffer, as well as whether the add succeeded.
func (rob *ReorderBuffer) Add(ts uint32, val float64) (schema.Point, []schema.Point, error) {
	ts = AggBoundary(ts, rob.interval)

	newestTs := rob.buf[rob.newest].Ts
	// out of order and too old
	if newestTs != 0 && ts < newestTs && newestTs-ts >= (uint32(cap(rob.buf))*rob.interval) {
		return schema.Point{}, nil, errors.ErrMetricTooOld
	}

	var firstPoint schema.Point
	var res []schema.Point
	oldest := (rob.newest + 1) % uint32(cap(rob.buf))
	index := (ts / rob.interval) % uint32(cap(rob.buf))
	if ts == rob.buf[index].Ts {
		if rob.allowUpdate {
			rob.buf[index].Ts = ts
			rob.buf[index].Val = val
		} else {
			return schema.Point{}, nil, errors.ErrMetricNewValueForTimestamp
		}
	} else if ts > newestTs {
		flushCount := (ts - newestTs) / rob.interval
		if flushCount > uint32(cap(rob.buf)) {
			flushCount = uint32(cap(rob.buf))
		}

		if flushCount > 1 {
			res = make([]schema.Point, 0, flushCount-1)
		}
		for i := uint32(0); i < flushCount; i++ {
			if rob.buf[oldest].Ts != 0 {
				if firstPoint.Ts == 0 {
					firstPoint = rob.buf[oldest]
				} else {
					res = append(res, rob.buf[oldest])
				}
				rob.buf[oldest].Ts = 0
			}
			oldest = (oldest + 1) % uint32(cap(rob.buf))
		}
		rob.buf[index].Ts = ts
		rob.buf[index].Val = val
		rob.newest = index
	} else {
		metricsReordered.Inc()
		rob.buf[index].Ts = ts
		rob.buf[index].Val = val
	}

	return firstPoint, res, nil
}

// Get returns the points in the buffer
func (rob *ReorderBuffer) Get() []schema.Point {
	res := make([]schema.Point, 0, cap(rob.buf))
	oldest := (rob.newest + 1) % uint32(cap(rob.buf))

	for {
		if rob.buf[oldest].Ts != 0 {
			res = append(res, rob.buf[oldest])
		}
		if oldest == rob.newest {
			break
		}
		oldest = (oldest + 1) % uint32(cap(rob.buf))
	}

	return res
}

func (rob *ReorderBuffer) Reset() {
	for i := range rob.buf {
		rob.buf[i].Ts = 0
	}
	rob.newest = 0
}

func (rob *ReorderBuffer) Flush() []schema.Point {
	res := rob.Get()
	rob.Reset()

	return res
}

func (rob *ReorderBuffer) IsEmpty() bool {
	return rob.buf[rob.newest].Ts == 0
}
